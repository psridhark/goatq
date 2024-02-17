package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/psridhark/goatq/client"
	"github.com/psridhark/goatq/core"
	"github.com/psridhark/goatq/core/config"
	"github.com/spf13/cobra"
	"github.com/tj/go-gracefully"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/psridhark/goatq/grpc"

	_ "net/http/pprof"
)

var (
	cli = &cobra.Command{
		Use:   "goatq",
		Short: "Distributed Msg Queue in Go and more",
	}

	brokerCfg = config.DefaultConfig()

	topicCfg = struct {
		Topics            string
		BrokerAddr        string
		Brokers           string
		GrpcAddr          string
		DataFile          string
		Topic             string
		Key               string
		CompressionType   string
		Partitions        int32
		ReplicationFactor int
		Message           string
		Offset            int32
		NumWorkers        int32
		NumMessages       uint64
		Limit             int64
		BatchCount        int
	}{}
)

func initBrokerCmd() {
	brokerCmd := &cobra.Command{Use: "broker", Short: "Run a goatq broker",
		Run: run, Args: cobra.NoArgs}
	brokerCmd.Flags().StringVar(&brokerCfg.DBRaftAddr, "db-raft-addr", "127.0.0.1:9093", "Address for Raft to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.RaftAddr, "raft-addr", "127.0.0.1:9093", "Address for Raft to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.HttpAddr, "http-addr", "127.0.0.1:8080", "Address for http to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.GrpcAddr, "grpc-addr",
		"127.0.0.1:50051",
		"Address for grpc to bind and advertise on")
	brokerCmd.Flags().StringVar(&brokerCfg.RaftDir, "raft-dir", "/tmp/goatq",
		"A comma separated list of directories under which to raft log files")
	brokerCmd.Flags().StringVar(&brokerCfg.DataDir, "data-dir", "/tmp/goatq",
		"A comma separated list of directories under which to store log files")
	brokerCmd.Flags().StringVar(&brokerCfg.Addr, "broker-addr", "0.0.0.0:9092", "Address for broker to bind on")
	brokerCmd.Flags().Var(newMemberlistConfigValue(brokerCfg.SerfLANConfig.MemberlistConfig, "0.0.0.0:9094"), "serf-addr", "Address for Serf to bind on")
	brokerCmd.Flags().BoolVar(&brokerCfg.Bootstrap, "bootstrap", false,
		"Initial cluster bootstrap (dangerous!)")
	brokerCmd.Flags().IntVar(&brokerCfg.BootstrapExpect, "bootstrap-expect", 0, "Expected number of nodes in cluster")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.StartJoinAddrsLAN, "join", nil, "Address of an broker serf to join at start time. Can be specified multiple times.")
	brokerCmd.Flags().StringSliceVar(&brokerCfg.StartJoinAddrsWAN, "join-wan", nil, "Address of an broker serf to join -wan at start time. Can be specified multiple times.")
	brokerCmd.Flags().Int32Var(&brokerCfg.ID, "id", 0, "broker ID")
	cli.AddCommand(brokerCmd)
}

func initTopicCmd() {
	topicCmd := &cobra.Command{Use: "topic", Short: "Manage topics"}
	topicCmd.Flags().StringVar(&topicCfg.BrokerAddr, "broker-addr",
		"0.0.0.0:9001", "Address for broker to bind on")
	topicCmd.Flags().StringVar(&topicCfg.GrpcAddr, "grpc-addr",
		"0.0.0.0:50051", "Address for the grpc server to bind on")

	createTopicCmd := &cobra.Command{Use: "create", Short: "Create a topic", Run: createTopic, Args: cobra.NoArgs}
	createTopicCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to create (required)")
	createTopicCmd.MarkFlagRequired("topic")
	createTopicCmd.Flags().Int32Var(&topicCfg.Partitions, "partitions", 1, "Number of partitions")
	createTopicCmd.Flags().IntVar(&topicCfg.ReplicationFactor, "replication-factor", 1, "Replication factor")
	createTopicCmd.Flags().StringVar(&topicCfg.CompressionType, "compression-type", "nocompression", "Compression type of topic ")
	createTopicCmd.Flags().StringVar(&topicCfg.GrpcAddr, "grpc-addr",
		"0.0.0.0:50051", "Address for the grpc server to bind on")

	produceMsgCmd := &cobra.Command{Use: "produce", Short: "Send a message",
		Run: produceMessage, Args: cobra.NoArgs}
	produceMsgCmd.Flags().StringVar(&topicCfg.Topic, "topic", "",
		"Name of topic to create (required)")
	_ = produceMsgCmd.MarkFlagRequired("topic")
	produceMsgCmd.Flags().StringVar(&topicCfg.Message, "message",
		"I am sending this", "Message to send")
	_ = produceMsgCmd.MarkFlagRequired("message")
	produceMsgCmd.Flags().StringVar(&topicCfg.GrpcAddr, "grpc-addr",
		"0.0.0.0:50051", "Address for the grpc server to bind on")

	consumeMsgCmd := &cobra.Command{Use: "consume", Short: "fetch message/s",
		Run: consumeMessage, Args: cobra.NoArgs}
	consumeMsgCmd.Flags().StringVar(&topicCfg.Topic, "topic", "", "Name of topic to create (required)")
	_ = consumeMsgCmd.MarkFlagRequired("topic")
	consumeMsgCmd.Flags().Int32Var(&topicCfg.Offset, "offset", 0,
		"read from a offset")
	_ = consumeMsgCmd.MarkFlagRequired("offset")
	consumeMsgCmd.Flags().Int32Var(&topicCfg.Partitions, "partitions", 0,
		"read from Partition")
	_ = consumeMsgCmd.MarkFlagRequired("partitions")
	consumeMsgCmd.Flags().Int64Var(&topicCfg.Limit, "limit", -1,
		"read from Partition")
	consumeMsgCmd.Flags().StringVar(&topicCfg.GrpcAddr, "grpc-addr",
		"0.0.0.0:50051", "Address for the grpc server to bind on")

	topicCmd.AddCommand(createTopicCmd)
	topicCmd.AddCommand(produceMsgCmd)
	topicCmd.AddCommand(consumeMsgCmd)

	cli.AddCommand(topicCmd)
}

func initBenchProduceCmd() {
	benchProduceMsgCmd := &cobra.Command{Use: "bench_produce", Short: "Send a message",
		Run: benchProduceMessage, Args: cobra.NoArgs}
	benchProduceMsgCmd.Flags().StringVar(&topicCfg.Topics, "topics", "",
		"Comma separated values of the topics")
	_ = benchProduceMsgCmd.MarkFlagRequired("topics")
	benchProduceMsgCmd.Flags().StringVar(&topicCfg.Brokers, "brokers",
		"0.0.0.0:9001", "comma separated brokers")
	benchProduceMsgCmd.Flags().StringVar(&topicCfg.DataFile, "data_file",
		"Data file", "Data file path")
	benchProduceMsgCmd.Flags().Int32Var(&topicCfg.NumWorkers, "num_workers", 1,
		"Number of workers")
	benchProduceMsgCmd.Flags().Uint64Var(&topicCfg.NumMessages, "num_messages", 1,
		"Number of Messages")
	benchProduceMsgCmd.Flags().IntVar(&topicCfg.BatchCount, "num_batch", 100,
		"Number of messages to batch")
	cli.AddCommand(benchProduceMsgCmd)
}

func init() {
	initBrokerCmd()
	initTopicCmd()
	initBenchProduceCmd()
}

func run(cmd *cobra.Command, args []string) {
	var err error

	log.SetPrefix(fmt.Sprintf("goatq: node id: %d: ", brokerCfg.ID))

	broker, err := core.NewNode(brokerCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error starting broker: %v\n", err)
		os.Exit(1)
	}

	go func() {
		fmt.Println(http.ListenAndServe(brokerCfg.HttpAddr, nil))
	}()

	_ = core.NewGrpcServer(brokerCfg.GrpcAddr, broker)

	fmt.Println("Initialized grpc server")

	gracefully.Timeout = 10 * time.Second
	gracefully.Shutdown()

	if err := broker.Shutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "error shutting down store: %v\n", err)
		os.Exit(1)
	}
}

func createTopic(cmd *cobra.Command, args []string) {
	conn, err := grpc.Dial(topicCfg.GrpcAddr, grpc.WithTransportCredentials(
		insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := pb.NewGoatQClient(conn)
	ctx := context.Background()
	configMap := make(map[string]string, 0)
	configMap["compression.type"] = topicCfg.CompressionType
	_, err = client.CreateTopic(ctx, &pb.TopicRequest{
		Topic:         topicCfg.Topic,
		NumPartitions: topicCfg.Partitions,
		ReplFactor:    int32(topicCfg.ReplicationFactor),
		Config:        configMap,
	})

	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully created topic %v\n", topicCfg.Topic)
}

func produceMessage(cmd *cobra.Command, args []string) {
	conn, err := grpc.Dial(topicCfg.GrpcAddr, grpc.WithTransportCredentials(
		insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := pb.NewGoatQClient(conn)
	ctx := context.Background()

	res, err := client.ProduceMessage(ctx, &pb.ProduceRequest{
		Topic: topicCfg.Topic,
		Record: &pb.DataRecord{
			Val: []byte(topicCfg.Message),
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Written message %v to parittion %v at offset %v\n",
		topicCfg.Message, res.Partition, res.Offset)

}

func consumeMessage(cmd *cobra.Command, args []string) {
	if topicCfg.Limit == -1 {
		goatqClient := client.NewGoatQClient(topicCfg.GrpcAddr)
		goatqClient.ConsumeMessage(topicCfg.Topic, uint64(topicCfg.Offset))
	} else {
		// Limit based
		conn, err := grpc.Dial(topicCfg.GrpcAddr, grpc.WithTransportCredentials(
			insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		client := pb.NewGoatQClient(conn)
		ctx := context.Background()

		resp, err := client.ConsumePartitionMessage(ctx, &pb.ConsumePartRequest{
			Topic:     topicCfg.Topic,
			Offset:    int64(topicCfg.Offset),
			Limit:     int64(topicCfg.Limit),
			Partition: topicCfg.Partitions,
		})

		if err != nil {
			fmt.Println("Got an Error :", err)
		} else {
			for _, record := range resp.Records {
				log.Printf("Received: %d,  %s\n", record.Offset, record.Val)
			}
		}

	}

}

func main() {
	err := cli.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed with err : %v", err)
		return
	}
}

type memberlistConfigValue memberlist.Config

func newMemberlistConfigValue(p *memberlist.Config, val string) (m *memberlistConfigValue) {
	m = (*memberlistConfigValue)(p)
	m.Set(val)
	return
}

func (v *memberlistConfigValue) Set(s string) error {
	bindIP, bindPort, err := net.SplitHostPort(s)
	if err != nil {
		return err
	}
	v.BindAddr = bindIP
	v.BindPort, err = strconv.Atoi(bindPort)
	if err != nil {
		return err
	}
	return nil
}

func (v *memberlistConfigValue) Type() string {
	return "string"
}

func (v *memberlistConfigValue) String() string {
	return fmt.Sprintf("%s:%d", v.BindAddr, v.BindPort)
}

func benchProduceMessage(cmd *cobra.Command, args []string) {
	// get the data from a file ?
	var runWg sync.WaitGroup
	topics := strings.Split(topicCfg.Topics, ",")
	brokers := strings.Split(topicCfg.Brokers, ",")
	dataFile := topicCfg.DataFile
	fmt.Printf("GOT TOPICs : %v\n GOT Brokers : %v\n", topics, brokers)
	for _, topic := range topics {
		for _, broker := range brokers {
			runWg.Add(1)
			topicName := topic
			brokerAddr := broker
			go func() {
				defer runWg.Done()
				benchProduceMessageWorker(brokerAddr, topicName, int(topicCfg.NumWorkers), topicCfg.NumMessages, topicCfg.BatchCount, dataFile)

			}()
		}
	}
	runWg.Wait()
}

func benchProduceMessageWorker(brokerAddr string, topicName string, numWorkers int, numMessages uint64, batchCount int, filePath string) {
	var wg sync.WaitGroup

	dataBuf := []string{}
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader to read the file line by line.
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)   // Set the buffer size (64 KB in this example).
	scanner.Buffer(buf, 10*1024*1024) //
	//fmt.Println(" Started Worker : ", workerID)
	// Iterate through the lines in the file
	for scanner.Scan() {
		dataBuf = append(dataBuf, scanner.Text())
	}

	i := 0
	for i < numWorkers {
		i += 1
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchProduceMessagePerBroker(brokerAddr, topicName, numMessages, batchCount, &dataBuf)

		}()
	}
	wg.Wait()

}

func produceMessagePerBroker(brokerGRPC string, topicName string, messageCount uint64) {
	conn, err := grpc.Dial(brokerGRPC, grpc.WithTransportCredentials(
		insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := pb.NewGoatQClient(conn)
	ctx := context.Background()
	i := 0
	for i < int(messageCount) {
		// Count the success

		i += 1
		message := fmt.Sprintf("%d", time.Now().UnixNano())
		client.ProduceMessage(ctx, &pb.ProduceRequest{
			Topic: topicName,
			Record: &pb.DataRecord{
				Key: []byte(message),
				Val: []byte(message),
			},
		})
		if i%1000 == 0 {
			fmt.Println("Wrote  to ", brokerGRPC, " : ", i)
		}

	}

	if err != nil {
		panic(err)
	}

}

func batchProduceMessagePerBrokerOld(brokerAddr string, topicName string, messageCount uint64, batchCount int) {
	msg := "Random"
	conn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(
		insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	maxSizeOption := grpc.MaxCallRecvMsgSize(64 * 10e6)
	client := pb.NewGoatQClient(conn)
	ctx := context.Background()
	i := 0
	req := &pb.ProduceBatchRequest{}
	req.Topic = topicName
	for i < int(messageCount) {
		// Count the success
		i += 1

		message := fmt.Sprintf("%d : %s", time.Now().UnixNano(), msg)
		//compressedMessage := util.CompressBytes([]byte(message))
		//req.Messages = append(req.Messages, compressedMessage)
		dataRecord := &pb.DataRecord{
			Val: []byte(message),
		}
		req.Records = append(req.Records, dataRecord)
		//fmt.Println("The size : ", i, " : ", len(req.Messages))
		if i%int(batchCount) == 0 {
			fmt.Println("Wrote  to ", topicName, " : on ", brokerAddr, " rows : ", i, " records : ", len(req.Records))
			_, err := client.ProduceMessageBatch(ctx, req, maxSizeOption)
			fmt.Println("YOV ERROR : ", err)
			req = &pb.ProduceBatchRequest{}
			req.Topic = topicName

		}

	}

	if err != nil {
		panic(err)
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

func getKeyRecordPair() (string, string) {
	return generateRandomString(30), generateRandomString(1024)

}

// read in memory
func batchProduceMessagePerBroker(brokerAddr string, topicName string, messageCount uint64, batchCount int, payLoad *[]string) {
	//msg := "{\"ChangedCols\": [\"open_heartbeat_time\"], \"IsDeleted\": false, \"Key\": \"f7019689fd3bd43e65885decda1261383434376166633133393839353163000187ff88\", \"Row\": {\"birth_time\": 1697430096, \"child_map\": null, \"directory_spec\": \"{\\\"1\\\":{\\\"i32\\\":3},\\\"2\\\":{\\\"lst\\\":[\\\"str\\\",0]},\\\"3\\\":{\\\"rec\\\":{\\\"1\\\":{\\\"i32\\\":1},\\\"2\\\":{\\\"i32\\\":2},\\\"3\\\":{\\\"i32\\\":1},\\\"4\\\":{\\\"i32\\\":67108864},\\\"5\\\":{\\\"i32\\\":0},\\\"6\\\":{\\\"i32\\\":0},\\\"7\\\":{\\\"i32\\\":0},\\\"8\\\":{\\\"i32\\\":0},\\\"9\\\":{\\\"lst\\\":[\\\"str\\\",0]}}},\\\"4\\\":{\\\"tf\\\":0},\\\"5\\\":{\\\"str\\\":\\\"\\\"},\\\"6\\\":{\\\"tf\\\":0},\\\"7\\\":{\\\"i64\\\":0},\\\"8\\\":{\\\"map\\\":[\\\"str\\\",\\\"rec\\\",0,{}]},\\\"9\\\":{\\\"str\\\":\\\"\\\"},\\\"10\\\":{\\\"tf\\\":1},\\\"12\\\":{\\\"tf\\\":0},\\\"13\\\":{\\\"tf\\\":0},\\\"14\\\":{\\\"rec\\\":{\\\"1\\\":{\\\"tf\\\":0},\\\"2\\\":{\\\"i64\\\":0}}},\\\"15\\\":{\\\"tf\\\":1},\\\"17\\\":{\\\"tf\\\":0}}\", \"lock\": null, \"open_heartbeat_time\": 1697430097, \"parent_map\": \"{\\\"1\\\":{\\\"map\\\":[\\\"str\\\",\\\"map\\\",1,{\\\"d24af39c0e7e1860\\\":[\\\"str\\\",\\\"i32\\\",1,{\\\"index\\\":2}]}]}}\", \"parent_uuid_hint\": \"d24af39c0e7e1860\", \"stripe_id\": -1, \"stripe_metadata\": null, \"symlink_target\": null, \"token__uuid\": 4311139349100883162, \"uuid\": \"a8447afc1398951c\"}, \"TableName\": \"files\", \"Timestamp\": {\"Logical\": 0, \"WallTime\": 1697430096994880503}}e\": 1697430096979151054}}"
	goatqClient := client.NewGoatQClient(brokerAddr)

	pmessages := []*client.ProducerMessage{}

	payloadLen := uint64(len(*payLoad))
	i := uint64(0)
	// Iterate through the lines in the file
	for i < messageCount {
		i += 1
		msgStr := (*payLoad)[i%payloadLen]
		pmessages = append(pmessages, &client.ProducerMessage{
			Key:   []byte(generateRandomString(30)),
			Value: []byte(msgStr),
		})
		//fmt.Println("The size : ", i, " : ", len(req.Messages))
		if i%uint64(batchCount) == 0 {
			fmt.Println("Wrote  to ", topicName, " : on ", brokerAddr, " rows : ", i, " records : ", len(pmessages))
			err := goatqClient.ProduceMessageBatch(topicName, pmessages)
			fmt.Println("YOV ERROR : ", err)
			pmessages = []*client.ProducerMessage{}
			//time.Sleep(time.Millisecond * 200)
		}

	}
	// Count the success

}
