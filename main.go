package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type ipHashTable map[byte]ipHashTable

var rootHashTable ipHashTable

var hashTablesCount int = 0

type KafkaMsg struct {
	SrcIP string `json:"srcip"`
	DstIP string `json:"dstip"`
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		//GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
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
	fmt.Print("Loading.")
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

	fmt.Println("\nNormal IP:", normalip, "Bad IP:", badip)

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

func main() {

	rootHashTable = make(ipHashTable, 255)
	loadIPfromFile("ip_list.txt")
	fmt.Printf("HashTables added %d\n", hashTablesCount)

	// get kafka reader using environment variables.
	kafkaURL := "10.5.92.15:9093" //os.Getenv("kafkaURL")
	topic := "hh"                 //os.Getenv("topic")
	groupID := "nogroup"          //os.Getenv("groupID")
	var msg KafkaMsg
	var MsgCount int64 = 0

	//c := make(chan os.Signal)
	//signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	reader := getKafkaReader(kafkaURL, topic, groupID)

	fmt.Println("start consuming ... !!")
	start := time.Now()

	defer reader.Close()

	for {
		MsgCount++
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		json.Unmarshal([]byte(m.Value), &msg)

		if search(parseIPtoArray(msg.SrcIP)) {
			fmt.Println("Found Bad IP in Src:", msg.SrcIP)
		}

		if search(parseIPtoArray(msg.DstIP)) {
			fmt.Println("Found Bad IP in Dst:", msg.DstIP)

		}

		//select {
		//case sig := <-c:
		//	fmt.Println("received message", sig)
		//		elapsed := time.Since(start)
		//		fmt.Printf("Message processed %b in %s\n", MsgCount, elapsed)
		//		break
		//	default:

		//	}

	}
	elapsed := time.Since(start)
	fmt.Printf("Message processed %b in %s\n", MsgCount, elapsed)

}
