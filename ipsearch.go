package main

import (
	"bufio"
	"context"
	json "encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var kafkaURL = flag.String("kafka-broker", "127.0.0.1:9092", "Kafka broker URL list")
var intopic = flag.String("kafka-in-topic", "notopic", "Kafka topic to read from")
var outtopic = flag.String("kafka-out-topic", "notopic", "Kafka topic to write to")
var groupID = flag.String("kafka-group", "nogroup", "Kafka group")
var filename = flag.String("ip-list", "badip.txt", "IP list to search")
var metricsport = flag.String("metric-port", "1234", "Port to expose metrics")

type ipHashTable map[byte]ipHashTable

var rootHashTable ipHashTable

var hashTablesCount int = 0

var logger *log.Logger

type kafkaMsg struct {
	CustomerID string `json:"type"`
	SrcIP      string `json:"srcip"`
	DstIP      string `json:"dstip"`
	Time       string `json:"@timestamp"`
	Action     string `json:"action"`
	BadIP      string `json:"badip"`
	//Message    string `json:"message"`
}

var reader *kafka.Reader
var writer *kafka.Writer

type stat_reader struct {
}

type stat_writer struct {
}

func (s *stat_reader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	str, _ := json.Marshal(reader.Stats())
	w.Write([]byte(str))
}

func (s *stat_writer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	str, _ := json.Marshal(writer.Stats())
	w.Write([]byte(str))
}

func getKafkaReader(kafkaURL, topic, groupID string, logger *log.Logger) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		StartOffset:    kafka.LastOffset,
		CommitInterval: 1 * time.Second,
		QueueCapacity:  10000,
		//ReadBackoffMin: 1 * time.Millisecond,
		ErrorLogger: logger,
	})
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaURL},
		Topic:   topic,
		//Balancer: &kafka.LeastBytes{},
		ErrorLogger: logger,
		Async:       true,
	})
}

func addIPToHashTable(root *ipHashTable, addr [4]byte, level byte) {

	newroot, found := (*root)[addr[level]]

	if !found {
		if level < 3 {
			(*root)[addr[level]] = make(ipHashTable, 255)
			hashTablesCount++
			addIPToHashTable(root, addr, level)
		} else {
			(*root)[addr[level]] = nil
			return
		}

	} else {
		if level == 3 {
			return
		}
		addIPToHashTable(&newroot, addr, level+1)
	}

}

func loadIPfromFile(fileName string) {

	file, err := os.Open(fileName)

	var normalip int = 0
	var badip int = 0

	if err != nil {
		logger.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	r, _ := regexp.Compile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)
	logger.Print("Loading.")

	for scanner.Scan() {
		str := scanner.Text()
		if r.MatchString(str) {
			addIPToHashTable(&rootHashTable, parseIPtoArray(str), 0)
			normalip++
			//fmt.Print(".")
		} else {
			logger.Print("Bad IP:", str)
			badip++
		}
	}

	logger.Print("Normal IP: ", normalip, " Bad IP: ", badip)

}

func parseIPtoArray(str string) [4]byte {

	var adr [4]byte

	strs := strings.Split(str, ".")

	if len(strs) != 4 {
		adr = [4]byte{0, 0, 0, 0}
		return adr
	}

	for i := 0; i < 4; i++ {
		x, _ := strconv.Atoi(strs[i])
		adr[i] = byte(x)
	}

	return adr
}

func search(adr [4]byte) bool {
	_, found := rootHashTable[adr[0]][adr[1]][adr[2]][adr[3]]
	return found
}

func init() {
	flag.Parse()
	logger = log.New(os.Stdout, "ipsearch: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {

	var msg kafkaMsg
	var msgcount uint32 = 0

	rootHashTable = make(ipHashTable, 255)

	loadIPfromFile(*filename)

	logger.Printf("HashTables added %d", hashTablesCount)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	reader = getKafkaReader(*kafkaURL, *intopic, *groupID, logger)
	writer = newKafkaWriter(*kafkaURL, *outtopic)

	quit := make(chan struct{})

	defer func() {
		reader.Close()
		writer.Close()
		close(quit)
	}()
	/*
		go func() {
			var st, delta uint32
			for {
				select {
				case <-quit:
					return
				default:
					st = msgcount
					time.Sleep(60 * time.Second)
					delta = msgcount - st
					logger.Printf("Processed %d messages per minute", delta)
				}
			}
		}()

	*/

	go func() {
		r := &stat_reader{}
		w := &stat_writer{}
		http.Handle("/metrics/reader", r)
		http.Handle("/metrics/writer", w)
		logger.Fatal(http.ListenAndServe(":"+*metricsport, nil))
	}()

	logger.Print("start consuming ... !!")

	start := time.Now()

	//  var readTime time.Duration = 0
	//	var searchTime time.Duration = 0
	//  var writeTime time.Duration = 0
loop:
	for {

		select {
		case sig := <-sigs:
			logger.Print(sig)
			break loop
		default:
			//startTime := time.Now()
			m, err := reader.ReadMessage(context.Background())
			//readTime = readTime + time.Since(startTime)

			msgcount++
			if err != nil {
				logger.Print(err)
			}

			err = msg.UnmarshalJSON([]byte(m.Value))

			if err == nil {
				//	startTime = time.Now()
				badsrc := search(parseIPtoArray(msg.SrcIP))
				baddst := search(parseIPtoArray(msg.DstIP))
				//	searchTime = searchTime + time.Since(startTime)

				action := false
				if msg.Action == "Accept" {
					action = true
				}

				if (badsrc && action) || baddst {

					if badsrc {
						msg.BadIP = msg.SrcIP

					}
					if baddst {
						msg.BadIP = msg.DstIP

					}

					alrm, _ := msg.MarshalJSON()

					str := kafka.Message{
						Key:   []byte("ti"), //[]byte(alert.BadIP)
						Value: alrm,
					}

					//		startTime = time.Now()
					err := writer.WriteMessages(context.Background(), str)
					//		writeTime = writeTime + time.Since(startTime)

					if err != nil {
						logger.Print(err)
					}

				}

			} else {
				logger.Print(err)
			}

		}
	}

	logger.Print("Terminating")
	elapsed := time.Since(start)
	logger.Printf("Message processed %d in %s", msgcount, elapsed)
	//logger.Printf("Read time: %s Search time: %s Write time %s", readTime, searchTime, writeTime)
}
