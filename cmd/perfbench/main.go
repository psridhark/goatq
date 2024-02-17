package main

import (
	"bufio"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/psridhark/goatq/client"
)

var SampleMessage = "{\"f1\" : \"v1\"}"

func readFromFile(filePath string) {
	// Specify the file path you want to read.
	// Open the file for reading.
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader to read the file line by line.
	scanner := bufio.NewScanner(file)

	// Iterate through the lines in the file.
	for scanner.Scan() {
		line := scanner.Text() // Get the current line.
		fmt.Println(line)      // Process the line (you can replace this with your own logic).
	}

	// Check for scanner errors.
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}

func generateRandomString(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charsetLen := big.NewInt(int64(len(charset)))
	randomString := make([]byte, size)

	for i := range randomString {
		n, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			panic(err)
		}
		randomString[i] = charset[n.Int64()]
	}

	return string(randomString)
}

func sendKafkarecords(workerID int, brokerAddr string, topicName string, messageCount uint64) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokerAddr, // kafka broker addr
	}
	// Create a writer to the Kafka topic.
	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	i := uint64(1)
	fmt.Println(" Started Worker : ", workerID)
	for i <= messageCount {
		msgStr := generateRandomString(1024)
		key := generateRandomString(30)

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          []byte(msgStr),
			Key:            []byte(key),
		}, nil)
		if err != nil {
			fmt.Printf("Failed to produce message %d: %v\n", i, err)
		}
		producer.Flush(3000)
		i += 1
		if i%1000 == 0 {

			fmt.Println("Pumped : ", i)
		}
	}
}

func sendKafkaBatch(workerID int, brokerAddr string, topicName string, filePath string, batchCount int) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": brokerAddr, // kafka broker addr
	}
	// Create a writer to the Kafka topic.
	producer, err := kafka.NewProducer(config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	i := uint64(1)
	fmt.Println(" Started Worker : ", workerID)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader to read the file line by line.
	scanner := bufio.NewScanner(file)

	// Iterate through the lines in the file
	for scanner.Scan() {
		msgStr := scanner.Text()
		key := generateRandomString(30)
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName, Partition: kafka.PartitionAny},
			Value:          []byte(msgStr),
			Key:            []byte(key),
		}, nil)
		if err != nil {
			fmt.Printf("Failed to produce message %d: %v\n", i, err)
		}

		i += 1
		if i%1000 == 0 {
			producer.Flush(3000)
			fmt.Println("Pumped : ", i)
		}
	}
}

func benchKafka(brokerAddr string, topicName string, workers int, messageCount uint64) {
	// Create a producer configuration.
	var wg sync.WaitGroup
	// Create a wait group for synchronization.
	i := 0

	for i < workers {
		i += 1
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendKafkarecords(workerID, brokerAddr, topicName, messageCount)
		}()

	}

	wg.Wait()
}

func benchKafkaBatch(brokerAddr string, topicName string, workers int, fileName string, batchCount int) {
	// Create a producer configuration.
	var wg sync.WaitGroup
	// Create a wait group for synchronization.
	i := 0

	for i < workers {
		i += 1
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendKafkaBatch(workerID, brokerAddr, topicName, fileName, batchCount)
		}()

	}

	wg.Wait()
}

func sendGoatQBatch(workerID int, brokerAddr string, topicName string, filePath string, batchCount int) {
	goatqClient := client.NewGoatQClient(brokerAddr)

	pmessages := []*client.ProducerMessage{}
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader to read the file line by line.
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)   // Set the buffer size (64 KB in this example).
	scanner.Buffer(buf, 10*1024*1024) //
	fmt.Println(" Started Worker : ", workerID)
	i := uint64(1)
	// Iterate through the lines in the file
	for scanner.Scan() {

		msgStr := scanner.Text()
		//fmt.Println(" In Worker : ", i, msgStr)
		key := generateRandomString(30)
		// Count the success

		//key, message := getKeyRecordPair()
		pmessages = append(pmessages, &client.ProducerMessage{
			Key:   []byte(key),
			Value: []byte(msgStr),
		})
		//fmt.Println("The size : ", i, " : ", len(req.Messages))
		i += 1
		if i%uint64(batchCount) == 0 {
			fmt.Println("Wrote  to ", topicName, " : on ", brokerAddr, " rows : ", i, " records : ", len(pmessages))
			err := goatqClient.ProduceMessageBatch(topicName, pmessages)
			fmt.Println("YOV ERROR : ", err)
			pmessages = []*client.ProducerMessage{}
		}

	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
}

func benchGoatQ(brokerAddr string, topicName string, workers int, fileName string, batchCount int) {
	var wg sync.WaitGroup
	// Create a wait group for synchronization.
	i := 0

	for i < workers {
		i += 1
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendGoatQBatch(workerID, brokerAddr, topicName, fileName, batchCount)
		}()

	}
	wg.Wait()
}

func main() {
	//---Define the various flags---
	mode := flag.String("mode", "goatq",
		"Mode of service")

	topic := flag.String("topic", "test1",
		"Name of the topic")

	workers := flag.Int("workers", 20,
		"Number of workers")

	messageCount := flag.Uint64("message_count", 1000000,
		"Number of messages")

	fileName := flag.String("data_file", "", "Data file to load from")

	batchCount := flag.Int("batch_count", 500,
		"Number of messages")

	brokerAddr := flag.String("broker", "127.0.0.1:9092",
		"broker Addr")

	flag.Parse()

	if *mode == "goatq" {
		benchGoatQ(*brokerAddr, *topic, *workers, *fileName, *batchCount)
	} else if *mode == "kafka" {
		benchKafka(*brokerAddr, *topic, *workers, *messageCount)
	} else if *mode == "kafka_batch" {
		benchKafkaBatch(*brokerAddr, *topic, *workers, *fileName, *batchCount)
	} else {
		fmt.Printf("Unknown mode : %s. Supported : goatq/kafka", *mode)
	}

}
