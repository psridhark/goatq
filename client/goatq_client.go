package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/psridhark/goatq/grpc"

	_ "net/http/pprof"
)

// GoatQClient
type GoatQClient struct {
	Broker string
	Conn   *grpc.ClientConn
	Client pb.GoatQClient
}

// ProducerMessage with key value byte sequence
type ProducerMessage struct {
	Key   []byte
	Value []byte
}

func NewGoatQClient(BrokerAddr string) *GoatQClient {
	conn, err := grpc.Dial(BrokerAddr, grpc.WithDefaultCallOptions(
		// TODO: make this configurable
		grpc.MaxCallRecvMsgSize(16*1024*1024), // 16MB
	),
		grpc.WithTransportCredentials(
			insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	client := pb.NewGoatQClient(conn)
	return &GoatQClient{
		Broker: BrokerAddr,
		Conn:   conn,
		Client: client,
	}
}

func (c *GoatQClient) CreateTopic(topicName string, numPartitions int, replicationFactor int, configMap map[string]string) error {
	ctx := context.Background()
	_, err := c.Client.CreateTopic(ctx, &pb.TopicRequest{
		Topic:         topicName,
		NumPartitions: int32(numPartitions),
		ReplFactor:    int32(replicationFactor),
		Config:        configMap,
	})

	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully created topic %v\n", topicName)
	return err
}

func (c *GoatQClient) GetTopicMetadata(topicName string) error {
	ctx := context.Background()
	topicMetadata, err := c.Client.GetTopicMetadata(ctx, &pb.GetTopicMetadataRequest{
		Topic:          topicName,
		WithPartitions: true,
	})

	if err != nil {
		panic(err)
	}
	fmt.Printf("Successfully Got topic Metadata %v\n", topicMetadata.Partitions)
	return err
}

func (c *GoatQClient) ProduceMessage(topicName string, pMessage *ProducerMessage) error {
	ctx := context.Background()
	// find Partiton to send to?

	topicMetadata, err := c.Client.GetTopicMetadata(ctx, &pb.GetTopicMetadataRequest{
		Topic:          topicName,
		WithPartitions: true,
	})
	if err != nil {
		panic(err)
	}
	numPartitions := len(topicMetadata.Partitions)
	res, err := c.Client.ProducePartitionMessage(ctx, &pb.ProducePartRequest{
		Topic:     topicName,
		Partition: int32(rand.Intn(numPartitions)), // random partition selection
		Record: &pb.DataRecord{
			Key: pMessage.Key,
			Val: pMessage.Value,
		},
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Written message %v to parittion %v at offset %v\n",
		topicName, res.Partition, res.Offset)

	return err

}

func (c *GoatQClient) ProduceMessageBatch(topicName string, pMessages []*ProducerMessage) error {
	ctx := context.Background()
	topicMetadata, err := c.Client.GetTopicMetadata(ctx, &pb.GetTopicMetadataRequest{
		Topic:          topicName,
		WithPartitions: true,
	})
	numPartitions := len(topicMetadata.Partitions)
	batchReqMap := make(map[int]*pb.ProduceBatchRequest)
	for _, partition := range topicMetadata.Partitions {
		req := &pb.ProduceBatchRequest{}
		req.Topic = topicName
		req.Partition = partition.Partition
		batchReqMap[int(partition.Partition)] = req

	}

	for _, pMessage := range pMessages {
		dataRecord := &pb.DataRecord{
			Key: pMessage.Key,
			Val: pMessage.Value,
		}
		partitonID := rand.Intn(numPartitions)
		batchReqMap[partitonID].Records = append(batchReqMap[partitonID].Records, dataRecord)

	}

	for _, req := range batchReqMap {
		_, err := c.Client.ProducePartitionMessageBatch(ctx, req)
		//fmt.Printf("Sent data to partition : %d : records : %d\n", partitionID, len(req.Records))
		if err != nil {
			fmt.Printf("Failed with error : %v\n", err)
		}

	}

	return err

}

func (c *GoatQClient) ConsumeMessage(topicName string, offset uint64) error {
	var runWg sync.WaitGroup
	ctx := context.Background()

	// Consume from all partitions

	topicMetadata, err := c.Client.GetTopicMetadata(ctx, &pb.GetTopicMetadataRequest{
		Topic:          topicName,
		WithPartitions: true,
	})

	if err != nil {
		fmt.Printf("Error in getting topic Metadata")
		return err
	}

	//
	for _, partition := range topicMetadata.Partitions {
		fmt.Println("Sent req to partition : ", partition.Partition)
		stream, err := c.Client.ConsumePartitionMessageStream(ctx, &pb.ConsumePartRequest{
			Topic:     topicName,
			Offset:    int64(offset),
			Partition: partition.Partition,
		})
		if err != nil {
			panic(err)
		}
		runWg.Add(1)
		streamCopy := stream

		go func() {
			defer runWg.Done()
			for {
				response, serr := streamCopy.Recv()
				if serr != nil {
					log.Fatalf("Error receiving response: %v", err)
				}
				for _, rec := range response.Records {
					log.Printf("Received: %d,  %s\n", rec.Offset, rec.Val)
				}

			}
		}()

	}

	runWg.Wait()
	return err
}
