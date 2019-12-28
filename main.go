package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

type ipHashTable map[byte]ipHashTable

var rootHashTable ipHashTable

var hashTablesCount int = 0

type kafkaMsg struct {
	SrcIP string `json:"srcip"`
	DstIP string `json:"dstip"`
}

type alarmMsg struct {
	Msg             string `json:"msg"`
	Direction       string `json:"direction"`
	BadIP           string `json:"badip"`
	SourceTimeStamp string `json:"time"`
	LogSource       string `json:"logsource"`
}

type server struct {
}

var reader *kafka.Reader

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	str, _ := json.Marshal(reader.Stats())
	w.Write([]byte(str))
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     3 * time.Second,
		StartOffset: kafka.LastOffset,
	})
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaURL},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
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
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	r, _ := regexp.Compile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)
	log.Print("Loading.")
	for scanner.Scan() {
		str := scanner.Text()
		if r.MatchString(str) {
			addIPToHashTable(&rootHashTable, parseIPtoArray(str), 0)
			normalip++
			//fmt.Print(".")
		} else {
			fmt.Println("\nBad IP:", str)
			badip++
		}
	}

	log.Println("\nNormal IP:", normalip, "Bad IP:", badip)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}

func parseIPtoArray(str string) [4]byte {

	var adr [4]byte

	strs := strings.Split(str, ".")

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
}

func main() {

	var msg kafkaMsg
	var alert alarmMsg

	rootHashTable = make(ipHashTable, 255)

	loadIPfromFile(*filename)

	log.Printf("HashTables added %d\n", hashTablesCount)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	reader = getKafkaReader(*kafkaURL, *intopic, *groupID)
	writer := newKafkaWriter(*kafkaURL, *outtopic)

	defer func() {
		reader.Close()
		writer.Close()
	}()

	go func() {
		s := &server{}
		http.Handle("/api", s)
		log.Fatal(http.ListenAndServe(":1234", nil))
	}()

	log.Println("start consuming ... !!")

	start := time.Now()

loop:
	for {

		select {
		case sig := <-sigs:
			log.Println(sig)
			break loop
		default:
			m, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Fatalln(err)
			}

			err = msg.UnmarshalJSON([]byte(m.Value))

			if err == nil {

				badsrc := search(parseIPtoArray(msg.SrcIP))
				baddst := search(parseIPtoArray(msg.DstIP))

				if badsrc || baddst {
					alert.Msg = "Suspicious IP found"
					alert.SourceTimeStamp = "11111"
					alert.LogSource = "self"
					if badsrc {
						alert.BadIP = msg.SrcIP
						alert.Direction = "inbound"
					}
					if baddst {
						alert.BadIP = msg.DstIP
						alert.Direction = "outound"
					}

					alrm, _ := alert.MarshalJSON()

					str := kafka.Message{
						Key:   []byte(alert.BadIP),
						Value: alrm,
					}

					err := writer.WriteMessages(context.Background(), str)

					if err != nil {
						fmt.Println(err)
					}

				}

			} else {
				log.Print(err)
			}

		}
	}

	log.Println("Terminating")
	elapsed := time.Since(start)
	log.Printf("Message processed %d in %s\n", reader.Stats().Messages, elapsed)

}
