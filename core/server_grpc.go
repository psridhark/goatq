package core

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/psridhark/goatq/commitlog"
	"github.com/psridhark/goatq/core/structs"
	pb "github.com/psridhark/goatq/grpc"
	"github.com/psridhark/goatq/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type GrpcServer struct {
	Count uint64
	b     *Node
	pb.GoatQServer
}

// Method to produce a single message on this broker.
func (g *GrpcServer) ProduceMessage(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse,
	error) {
	g.Count += 1
	log.Debug.Printf("broker/%d: produce: %#v", g.b.config.ID, *req)
	res := &pb.ProduceResponse{}
	err := g.b.withTimeout(reqTimeout, func() error {
		state := g.b.fsm.State()
		_, t, err := state.GetTopic(req.Topic)
		if err != nil {
			log.Error.Printf("broker/%d: produce to partition error: get"+
				" topic: %s", g.b.config.ID, err)
			return err
		}
		if t == nil {
			log.Error.Printf("broker/%d: produce to partition error: unknown"+
				" topic", g.b.config.ID)
			return errors.New("unknown topic")
		}
		numPar := len(t.Partitions)
		// Hash it based on the key else random pump
		partitionID := int32(rand.Intn(numPar))
		partRes, err := g.ProducePartitionMessage(ctx, &pb.ProducePartRequest{
			Topic:     req.Topic,
			Partition: partitionID,
			Record:    req.Record,
		})

		if err != nil {
			return errors.New("Error writing to replica leader for this partition")
		}
		res.Offset = partRes.Offset
		res.Partition = partRes.Partition
		return nil
	})

	return res, err
}

// TODO: fix this method
// Method to produce a message to a specifc partition of a topic.
func (g *GrpcServer) ProducePartitionMessage(ctx context.Context, req *pb.ProducePartRequest) (*pb.ProducePartResponse,
	error) {

	// Just test the raft here?
	log.Debug.Printf("broker/%d: produce: %#v", g.b.config.ID, *req)
	res := &pb.ProducePartResponse{}
	err := g.b.withTimeout(reqTimeout, func() error {
		state := g.b.fsm.State()
		_, t, err := state.GetTopic(req.Topic)
		if err != nil {
			log.Error.Printf("broker/%d: produce to partition error: get"+
				" topic: %s", g.b.config.ID, err)
			return err
		}
		if t == nil {
			log.Error.Printf("broker/%d: produce to partition error: unknown"+
				" topic", g.b.config.ID)
			return errors.New("unknown topic")
		}
		replica, err := g.b.replicaLookup.Replica(req.Topic,
			int32(req.Partition))
		if err != nil || replica == nil || replica.Log == nil {
			log.Error.Printf("broker/%d: produce to partition error: %s",
				g.b.config.ID, err)
			return errors.New("replica not available")
		}

		replicaRaft := g.b.dbRaft.GetNoOPSession(uint64(replica.Partition.RaftGroupID))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		// convert into bytes
		recordBytes, err := proto.Marshal(req.Record)
		if err != nil {
			return errors.New("proto marshalling error")
		}
		smResult, err := g.b.dbRaft.SyncPropose(ctx, replicaRaft, recordBytes)

		replicaRaft.ProposalCompleted()
		res.Offset = int64(smResult.Value)
		res.Partition = req.Partition
		return nil
	})

	return res, err
}

// Method to produce a batch message to a random partition
func (g *GrpcServer) ProduceMessageBatch(ctx context.Context, req *pb.ProduceBatchRequest) (*pb.ProduceBatchResponse,
	error) {
	res := &pb.ProduceBatchResponse{}
	state := g.b.fsm.State()
	_, t, err := state.GetTopic(req.Topic)
	if err != nil {
		log.Error.Printf("broker/%d: produce to partition error: get"+
			" topic: %s", g.b.config.ID, err)
		return res, err
	}
	if t == nil {
		log.Error.Printf("broker/%d: produce to partition error: unknown"+
			" topic", g.b.config.ID)
		return res, errors.New("unknown topic")
	}
	numPar := len(t.Partitions)
	partitionID := int32(rand.Intn(numPar))

	err = g.b.ProduceRecordsForPartition(req.Topic, partitionID, req.Records)

	return res, err
}

// Method to produce a message batch to a specific partition of a topic.
func (g *GrpcServer) ProducePartitionMessageBatch(ctx context.Context, req *pb.ProduceBatchRequest) (*pb.ProduceBatchResponse,
	error) {
	res := &pb.ProduceBatchResponse{}

	err := g.b.ProduceRecordsForPartition(req.Topic, req.Partition, req.Records)

	// TODO : Fill the offset ?
	//res.Offset = int64(smResult.Value)

	return res, err
}

func (g *GrpcServer) ConsumeMessage(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse,
	error) {
	log.Debug.Printf("In ConsumeMessage: %v\n", req)
	_, meta, s := g.b.fsm.State().GetTopic(req.Topic)
	if s != nil {
		return nil, s
	}
	resp := &pb.ConsumeResponse{}

	// Get from all partitions and close connections ?

	for partition, _ := range meta.Partitions {
		replica, err := g.b.replicaLookup.Replica(req.Topic, partition)
		_, replicaLeader, err := g.b.fsm.State().GetNode(replica.Partition.Leader)
		if err != nil {
			return nil, errors.New("Error getting replica leader for this partition, ")
		}
		conn, err := grpc.Dial(replicaLeader.GRPCAddress, grpc.WithTransportCredentials(
			insecure.NewCredentials()))
		if err != nil {
			return nil, errors.New("Error Connecting to replica leader for this partition")
		}

		client := pb.NewGoatQClient(conn)
		ctx := context.Background()
		partRes, err := client.ConsumePartitionMessage(ctx, &pb.ConsumePartRequest{
			Topic:     req.Topic,
			Partition: partition,
			Offset:    req.Offset,
		})
		if err != nil {
			log.Error.Println("THE ERROR IS : ", err)
		} else {
			if len(partRes.Records) > 0 {
				resp.Records = append(resp.Records, partRes.Records...)
			}
		}

	}

	return resp, nil
}

func (g *GrpcServer) ConsumePartitionMessage(ctx context.Context, req *pb.ConsumePartRequest) (*pb.ConsumePartResponse,
	error) {
	// fmt.Printf("IN ConsumePartitionMessage: %v\n", req)
	limit := int(req.Limit)
	resp := &pb.ConsumePartResponse{}
	replica, err := g.b.replicaLookup.Replica(req.Topic, req.Partition)
	if err != nil {
		return resp, errors.New("replica not available")
	}
	//if replica.Partition.Leader != g.b.config.ID {
	//	return errors.New("replica not leader for the partition")
	//}
	if replica.Log == nil {
		return resp, errors.New("replica log not available")
	}
	//fmt.Println("GETTING THE READER for offset : ", req.Offset)
	rdr, rdrErr := replica.Log.NewLogReader(req.Offset)
	if rdrErr != nil {
		//log.Error.Printf("broker/%d: replica log read error: %s",
		//	g.b.config.ID, rdrErr)
		return resp, rdrErr
	}
	fetchCount := 0

	for fetchCount < limit {
		records, err := rdr.ReadRecordSet()
		if err != nil {
			return resp, err
		}
		resp.Records = append(resp.Records, records...)
		fetchCount += len(records)
	}

	fmt.Println("GOT records : ", len(resp.Records))
	// Parse the Record and respond get the offset too?

	return resp, nil
}

func (g *GrpcServer) ConsumePartitionMessageStream(req *pb.ConsumePartRequest, stream pb.GoatQ_ConsumePartitionMessageStreamServer) error {
	fmt.Printf("In Consume: %v\n", req)
	//var record commitlog.Record

	replica, err := g.b.replicaLookup.Replica(req.Topic, req.Partition)
	if err != nil {
		return errors.New("replica not available")
	}
	//if replica.Partition.Leader != g.b.config.ID {
	//	return errors.New("replica not leader for the partition")
	//}
	if replica.Log == nil {
		return errors.New("replica log not available")
	}
	rdr, rdrErr := replica.Log.NewLogReader(req.Offset)
	if rdrErr != nil {
		log.Error.Printf("broker/%d: replica log read error: %s",
			g.b.config.ID, rdrErr)
		return rdrErr
	}
	offsetStreamed := req.Offset
	for {
		select {
		case <-stream.Context().Done():
			fmt.Println("Client connection closed. Ending stream.")
			return nil
		default:
			//record, err = rdr.ReadRecord()
			// recordBytes, err := rdr.ReadRecordBytes()
			//recordProto, err := rdr.ReadRecordProto()
			records, err := rdr.ReadRecordSet()
			// if we hit the end of segment , refresh the segment using the offset and read again
			if err == commitlog.ErrEndOfSegment {
				// reload a reader
				log.Error.Printf("Reloading Reader : \n")
				newOffset := offsetStreamed + 1 // new offset since last recvd
				rdr, rdrErr = replica.Log.NewLogReader(newOffset)
				if rdrErr != nil {
					log.Error.Printf("broker/%d: replica log read error: %s",
						g.b.config.ID, rdrErr)
					return rdrErr
				}
			} else if err != nil {
				log.Error.Printf("Streaming Err : ", err)
				time.Sleep(2 * time.Second)
				continue
			} else {

				/*
					record := &pb.DataRecord{}
					if proto.Unmarshal(recordBytes, record) != nil {
						log.Error.Printf("Proto Unmarshal Err : ", err)
					}
					//msg := // create a recordType
					offsetStreamed += 1
				*/
				if len(records) > 0 {
					offsetStreamed = int64(records[len(records)-1].Offset)
					resp := &pb.ConsumePartResponse{}
					//resp.Records = append(resp.Records, recordProto)
					resp.Records = records
					stream.Send(resp)
				}

			}
		}
	}

	return nil
}

// Method to create a topic.
func (g *GrpcServer) CreateTopic(ctx context.Context,
	rq *pb.TopicRequest) (*pb.TopicResponse, error) {

	// Translate the topic config to config params
	// TODO: Get a global count to use it as the raft Group ID
	err := g.b.withTimeout(reqTimeout, func() error {
		fmt.Println("In createTopic")
		state := g.b.fsm.State()
		_, t, _ := state.GetTopic(rq.Topic)
		if t != nil {
			return errors.New("topic already exists")
		}
		ps, err := g.b.buildPartitions(rq.Topic, rq.NumPartitions,
			int16(rq.ReplFactor))
		if err != nil {
			return err
		}

		tt := structs.Topic{
			Topic:      rq.Topic,
			Partitions: make(map[int32][]int32),
			Config:     rq.Config,
		}
		for _, partition := range ps {
			tt.Partitions[partition.ID] = partition.AR
		}
		// TODO: create/set topic config here
		if _, err := g.b.raftApply(structs.RegisterTopicRequestType,
			structs.RegisterTopicRequest{Topic: tt}); err != nil {
			return err
		}
		for _, partition := range ps {
			if err := g.b.createPartition(partition); err != nil {
				return err
			}
		}

		return nil
	})
	return &pb.TopicResponse{}, err
}

func (g *GrpcServer) DeleteTopic(ctx context.Context,
	rq *pb.TopicRequest) (*pb.TopicResponse, error) {
	isController := g.b.isController()
	if !isController {
		return nil, errors.New("broker not the controller")
	}
	err := g.b.withTimeout(10000, func() error {
		// TODO: this will delete from fsm -- need to delete associated partitions, etc.
		_, err := g.b.raftApply(structs.DeregisterTopicRequestType,
			structs.DeregisterTopicRequest{
				Topic: structs.Topic{
					Topic: rq.Topic,
				},
			})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &pb.TopicResponse{}, err
}

// Method to list the topics.
func (g *GrpcServer) ListTopics(ctx context.Context,
	rq *pb.ListTopicRequest) (*pb.ListTopicResponse, error) {

	res := &pb.ListTopicResponse{}
	res.Topics = make(map[string]*pb.GetTopicMetadataResponse)

	_, topics, err := g.b.fsm.State().GetTopics()
	if err != nil {
		return nil, nil
	}
	for _, topic := range topics {
		partitions := make([]*pb.PartitionMetadata, 0)
		for partition, replicas := range topic.Partitions {
			repl, _ := g.b.replicaLookup.Replica(topic.Topic, partition)
			metadata := &pb.PartitionMetadata{
				Partition: partition,
				Replicas:  replicas,
				Leader:    repl.Partition.Leader,
			}
			partitions = append(partitions, metadata)
		}
		res.Topics[topic.Topic] = &pb.GetTopicMetadataResponse{
			Partitions: partitions,
		}
	}

	return res, nil
}

// Method to get the Topic Metadata.
func (g *GrpcServer) GetTopicMetadata(ctx context.Context,
	rq *pb.GetTopicMetadataRequest) (*pb.GetTopicMetadataResponse, error) {
	_, meta, s := g.b.fsm.State().GetTopic(rq.Topic)
	if s != nil {
		return nil, s
	}
	res := &pb.GetTopicMetadataResponse{}
	res.Partitions = make([]*pb.PartitionMetadata, 0)
	for partition, replicas := range meta.Partitions {
		repl, _ := g.b.replicaLookup.Replica(rq.Topic, partition)
		metadata := &pb.PartitionMetadata{
			Partition: partition,
			Replicas:  replicas,
			Leader:    repl.Partition.Leader,
		}
		res.Partitions = append(res.Partitions, metadata)
	}
	return res, nil
}

// New instance of gRPC server.
func NewGrpcServer(grpcAddr string, b *Node) *GrpcServer {
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Error.Printf("failed to listen: %v", err)
	}
	srv := &GrpcServer{b: b}
	s := grpc.NewServer(
		grpc.MaxRecvMsgSize(32 * 1024 * 1024),
	)
	pb.RegisterGoatQServer(s, srv)

	//
	if err := s.Serve(lis); err != nil {
		log.Error.Printf("failed to serve: %v", err)
	}
	return srv
}
