package core

import (
	"bytes"
	"container/ring"
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/hashicorp/serf/serf"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dragonboatconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/statemachine"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/psridhark/goatq/commitlog"
	"github.com/psridhark/goatq/core/config"
	"github.com/psridhark/goatq/core/fsm"
	"github.com/psridhark/goatq/core/metadata"
	"github.com/psridhark/goatq/core/replica"
	repl "github.com/psridhark/goatq/core/replica"
	"github.com/psridhark/goatq/core/structs"
	pb "github.com/psridhark/goatq/grpc"
	"github.com/psridhark/goatq/log"
)

var (
	brokerVerboseLogs bool

	ErrTopicExists            = errors.New("topic exists already")
	ErrInvalidArgument        = errors.New("no logger set")
	OffsetsTopicName          = "__consumer_offsets"
	OffsetsTopicNumPartitions = 50
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	raftState         = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
	reqTimeout        = time.Second * 5
)

func init() {
	spew.Config.Indent = ""

	e := os.Getenv("GOATQDEBUG")
	if strings.Contains(e, "broker=1") {
		brokerVerboseLogs = true
	}
}

// Node represents a broker in a GoatQ cluster, like a broker in a Kafka cluster.
type Node struct {
	sync.RWMutex
	config *config.Config

	// readyForConsistentReads is used to track when the leader server is
	// ready to serve consistent reads, after it has applied its initial
	// barrier. This is updated atomically.
	readyForConsistentReads int32
	// brokerLookup tracks servers in the local datacenter.
	brokerLookup  *BrokerLookup
	replicaLookup *replica.ReplicaLookup
	// The raft instance is used among GoatQ brokers within the DC to protect operations that require strong consistency.
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftInmem     *raft.InmemStore
	// raftNotifyCh ensures we get reliable leader transition notifications from the raft layer.
	raftNotifyCh <-chan bool
	// reconcileCh is used to pass events from the serf handler to the raft leader to update its state.
	reconcileCh      chan serf.Member
	dbRaft           *dragonboat.NodeHost
	raftClient       *client.Session
	serf             *serf.Serf
	fsm              *fsm.FSM
	eventChLAN       chan serf.Event
	logStateInterval time.Duration
	tracer           opentracing.Tracer

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// NewNode is used to instantiate a new broker.
func NewNode(config *config.Config) (*Node, error) {
	b := &Node{
		config:           config,
		shutdownCh:       make(chan struct{}),
		eventChLAN:       make(chan serf.Event, 256),
		brokerLookup:     NewBrokerLookup(),
		replicaLookup:    replica.NewReplicaLookup(),
		reconcileCh:      make(chan serf.Member, 32),
		logStateInterval: time.Second * 60,
	}

	log.SetPrefix(fmt.Sprintf("broker/%d: ", b.config.ID))

	var err error

	defer func() {
		if err != nil {
			_ = b.Shutdown()
		}
	}()

	// setupRaft for metadata
	if err = b.setupRaft(); err != nil {
		return nil, fmt.Errorf("start raft: %v", err)
	}

	// setup serf for gossip
	b.serf, err = b.setupSerf(config.SerfLANConfig, b.eventChLAN, serfLANSnapshot)
	if err != nil {
		return nil, err
	}

	// setupRaft for metadata
	if err = b.setupDragonBoatRaft(); err != nil {
		return nil, fmt.Errorf("start dragonboat raft: %v", err)
	}

	b.JoinLAN(b.config.StartJoinAddrsLAN...)

	go b.lanEventHandler()

	go b.monitorLeadership()

	go b.logState()

	go b.logMemUsage()

	go b.processReplicaCh()

	return b, err
}

func (b *Node) logMemUsage() {
	t := time.NewTicker(b.logStateInterval)
	for {
		select {
		case <-b.shutdownCh:
			return
		case <-t.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Println("-----")
			//fmt.Printf("Alloc: %d bytes\n", m.Alloc)
			//fmt.Printf("TotalAlloc: %d bytes\n", m.TotalAlloc)
			fmt.Printf("Sys: %d MB\n", m.Sys/(1024*1024))
			fmt.Printf("NumGC: %d \n", m.NumGC)
			fmt.Printf("HeapAlloc: %d MB\n", m.HeapAlloc/(1024*1024))
			fmt.Printf("HeapInUse: %d MB\n", m.HeapInuse/(1024*1024))
			fmt.Printf("HeapIdle: %d MB\n", m.HeapIdle/(1024*1024))
			fmt.Printf("HeapReleased: %d MB\n", m.HeapReleased/(1024*1024))
			fmt.Printf("StackInuse: %d MB\n", m.StackInuse/(1024*1024))
			fmt.Printf("StackSys: %d MB\n", m.StackSys/(1024*1024))
			fmt.Printf("NextGC: %d MB\n", m.NextGC)
			fmt.Println("------")

			// Optionally, you can also print other memory-related statistics:
			// - m.HeapInuse
			// - m.HeapIdle
			// - m.HeapReleased
			// - m.StackInuse
			// - m.StackSys
			// - ...

		}
	}
}

// JoinLAN Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Node) JoinLAN(addrs ...string) error {
	fmt.Println("in JoinLAN : ", addrs)
	nodeContacted, err := b.serf.Join(addrs, true)
	if err != nil {
		return err
	}
	fmt.Println("SUCCESSFULLY CONATCTED NODE : ", nodeContacted)
	return nil
}

// TO setup the DragonBoat Raft Layer for creating raft groups in future.
func (b *Node) setupDragonBoatRaft() (err error) {
	// Setup a raft layer for multi Raft groups..

	datadir := filepath.Join(
		b.config.RaftDir,
		"db-raft-data",
		fmt.Sprintf("node%d", b.config.ID))

	log.Info.Printf("STARTING DB RAFT AT : %s\n", b.config.DBRaftAddr)
	lbConfig := dragonboatconfig.GetDefaultLogDBConfig()
	lbConfig.KVWriteBufferSize = 8 * 1024 * 1204 // Memtable size
	lbConfig.Shards = 1                          // number of pebble instances
	lbConfig.KVMaxWriteBufferNumber = 4          // Number of memtables
	lbConfig.KVLRUCacheSize = 4 * 1024 * 1024
	//lbConfig.KVLRUCacheSize =
	nhc := dragonboatconfig.NodeHostConfig{
		//	WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 1000,
		RaftAddress:    b.config.DBRaftAddr,
		Expert:         dragonboatconfig.ExpertConfig{LogDB: lbConfig},
		//RaftRPCFactory: rpc.NewRaftGRPC,
	}
	// create a NodeHost instance. it is a facade interface allowing access to
	// all functionalities provided by dragonboat.
	nh, err := dragonboat.NewNodeHost(nhc)
	b.dbRaft = nh
	return err
}

// processReplicaCh will be used process the replica partitions coming fron
// raftlog replay
func (b *Node) processReplicaCh() {
	time.Sleep(time.Second * 10)
	for replica := range b.fsm.ReplicaCh {
		b.replicaLookup.AddReplica(replica)

		// Get the path of Data

		// used only for reading
		if err := b.setupReplicaLog(replica); err != nil {
			log.Error.Printf("error starting replica : %v\n", err)
			continue
		}
		// Setup DragonBoat replica Raft
		b.setupReplicaRaftGroup(replica)
	}
}

// Method to setup the raft group for a replica/partition when we create a topic
// or initialize on restart
func (b *Node) setupReplicaRaftGroup(replica *replica.Replica) error {
	log.Info.Printf("setupReplicaRaft for topic : %s\n", replica.Partition.Topic)
	// get address of all holders
	initialMembers := make(map[uint64]string)

	for _, brokerID := range replica.Partition.AR {
		brokerObject := b.brokerLookup.BrokerByID(raft.ServerID(fmt.Sprintf("%d",
			brokerID)))
		log.Info.Printf("GOT THE broker : %s\n", brokerObject.DBRaftAddr)
		initialMembers[uint64(brokerID)] = brokerObject.DBRaftAddr
	}

	log.Info.Printf("CREATING the raftGroup on node %d :  partition : %d : %d\n", b.config.ID, replica.Partition.ID, replica.Partition.RaftGroupID)
	// TODO: dynamic change of the settings?
	rc := dragonboatconfig.Config{
		ReplicaID:               uint64(b.config.ID),                   // node ID
		ShardID:                 uint64(replica.Partition.RaftGroupID), // partitionID
		ElectionRTT:             5,                                     // 5 seconds?
		HeartbeatRTT:            2,                                     // 2 seconds?
		CheckQuorum:             false,                                 // No need for the leader to check repeatedly
		SnapshotEntries:         1000,
		CompactionOverhead:      100,
		Quiesce:                 true,
		SnapshotCompressionType: dragonboatconfig.Snappy,
		EntryCompressionType:    dragonboatconfig.Snappy,
	}

	// PebbleFSM
	/*
		keyPrefix := fmt.Sprintf("%s/%d", replica.Partition.Topic, replica.Partition.ID)
		createFSMFunc := func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
			return NewPebbleFSM(b.dataStore, uint64(replica.Partition.RaftGroupID), keyPrefix)
		}

		if err := b.dbRaft.StartOnDiskReplica(initialMembers, false, createFSMFunc, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			return err
		}
	*/

	// CommitLog FSM on Disk

	createFSMFunc := func(shardID uint64, replicaID uint64) statemachine.IOnDiskStateMachine {
		return repl.NewCommitLogFSM(replica.Partition.Topic, replica.Log, uint64(replica.Partition.RaftGroupID), uint64(b.config.ID))
	}

	if err := b.dbRaft.StartOnDiskReplica(initialMembers, false, createFSMFunc, rc); err != nil {
		log.Error.Printf("failed to add cluster, %v\n", err)
		return err
	}

	/*
		createFSMFunc := func(shardID uint64, replicaID uint64) statemachine.IStateMachine {
			return NewReplicaFSM(replica.Partition.Topic, replica.Log, uint64(replica.Partition.RaftGroupID), uint64(b.config.ID))

		}

		if err := b.dbRaft.StartReplica(initialMembers, false, createFSMFunc, rc); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			return err
		}

	*/
	return nil
}

// isController returns true if this is the cluster controller.
func (b *Node) isController() bool {
	return b.isLeader()
}

func (b *Node) isLeader() bool {
	return b.raft.State() == raft.Leader
}

// createPartition is used to add a partition across the cluster.
func (b *Node) createPartition(partition structs.Partition) error {
	_, err := b.raftApply(structs.RegisterPartitionRequestType, structs.RegisterPartitionRequest{
		Partition: partition,
	})
	return err
}

// setupReplicaLog is used to setup a replica on this broker, including creating its commit log.
func (b *Node) setupReplicaLog(replica *replica.Replica) error {
	b.Lock()
	defer b.Unlock()

	state := b.fsm.State()
	_, topic, _ := state.GetTopic(replica.Partition.Topic)

	if topic == nil {
		log.Info.Printf("start replica called on unknown topic: %s", replica.Partition.Topic)
		return errors.New("Unknown topic")
	}

	logPath := filepath.Join(b.config.DataDir, replica.Partition.Topic, fmt.Sprintf("%d", replica.Partition.ID))
	if replica.Log == nil {

		cleanupPolicy := commitlog.DeleteCleanupPolicy
		compressionType := commitlog.NoCompression
		for key := range topic.Config {
			if key == "cleanup.policy" {
				cleanupPolicy = topic.Config[key]
			} else if key == "compression.type" {
				compressionType = topic.Config[key]
			}
		}

		log, err := commitlog.New(commitlog.Options{
			Path:            logPath,
			MaxSegmentBytes: 102400000, // 1GB
			MaxLogBytes:     -1,
			CleanupPolicy:   commitlog.CleanupPolicy(cleanupPolicy),
			CompressionType: commitlog.CompressionType(compressionType),
		})
		if err != nil {
			return err
		}
		replica.Log = log
		// TODO: register leader-change listener on r.replica.Partition.id
	}

	return nil
}

// This will produce the records for the partition by encoding the dataRecords into
// record set Bytes and applying it via the raft proposal for the partition
func (b *Node) ProduceRecordsForPartition(topicName string, partitionID int32, records []*pb.DataRecord) error {
	now := time.Now()
	replica, err := b.replicaLookup.Replica(topicName,
		int32(partitionID))
	if err != nil || replica == nil || replica.Log == nil {
		log.Error.Printf("produce to partition error: %s", err)
		return errors.New("replica not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	replicaRaft := b.dbRaft.GetNoOPSession(uint64(replica.Partition.RaftGroupID))

	// Encode the records into Bytes here itself so we avoid any more processing until reading it.
	recordSetBytes, err := replica.Log.EncodeRecordSet(0, records)
	if err != nil {
		log.Error.Println("Error in encoding recordSet for Raft")
		return err
	}

	_, err = b.dbRaft.SyncPropose(ctx, replicaRaft, recordSetBytes)
	if err != nil {
		log.Error.Println("Proposal Error : ", err, time.Since(now))
		return err
	} else {
		log.Info.Printf("Time taken for proposal : %v\n", time.Since(now))
	}

	return nil

}

// Method to build partitions when we create a new topic.
func (b *Node) buildPartitions(topic string, partitionsCount int32,
	replicationFactor int16) ([]structs.Partition, error) {
	brokers := b.brokerLookup.Brokers()
	count := len(brokers)

	if int(replicationFactor) > count {
		return nil, errors.New("Invalid replication factor")
	}

	// container/ring is dope af
	r := ring.New(count)
	for i := 0; i < r.Len(); i++ {
		r.Value = brokers[i]
		r = r.Next()
	}

	var partitions []structs.Partition

	for i := int32(0); i < partitionsCount; i++ {
		// TODO: maybe just go next here too
		r = r.Move(rand.Intn(count))
		leader := r.Value.(*metadata.Broker)
		replicas := []int32{leader.ID.Int32()}
		for i := int16(0); i < replicationFactor-1; i++ {
			r = r.Next()
			replicas = append(replicas, r.Value.(*metadata.Broker).ID.Int32())
		}
		// Generate RaftGroupID
		raftGroupID := b.getRaftGroupID(topic, i)
		partition := structs.Partition{
			Topic:       topic,
			ID:          i,
			Partition:   i,
			RaftGroupID: raftGroupID,
			Leader:      leader.ID.Int32(),
			AR:          replicas,
			ISR:         replicas,
		}
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// Leave is used to prepare for a graceful shutdown.
func (b *Node) Leave() error {
	log.Info.Printf("starting leave")

	numPeers, err := b.numPeers()
	if err != nil {
		log.Error.Printf("check raft peers error: %s", err)
		return err
	}

	isLeader := b.isLeader()
	if isLeader && numPeers > 1 {
		future := b.raft.RemoveServer(raft.ServerID(fmt.Sprintf("%d", b.config.ID)), 0, 0)
		if err := future.Error(); err != nil {
			log.Error.Printf("remove ourself as raft peer error: %s", err)
		}
	}

	if b.serf != nil {
		if err := b.serf.Leave(); err != nil {
			log.Error.Printf("leave LAN serf cluster error: %s", err)
		}
	}

	time.Sleep(b.config.LeaveDrainTime)

	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := b.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				log.Error.Printf("get raft configuration error: %s", err)
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == raft.ServerAddress(b.config.RaftAddr) {
					left = false
					break
				}
			}
		}
	}

	return nil
}

// Shutdown is used to shutdown the broker, its serf, its raft, and so on.
func (b *Node) Shutdown() error {
	log.Info.Printf("shutting down broker")
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	if b.serf != nil {
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Error.Printf("shutdown error: %s", err)
		}
		if b.raftStore != nil {
			b.raftStore.Close()
		}
	}

	return nil
}

func (b *Node) getRaftGroupID(topic string, partitionID int32) uint32 {
	TopicPartitionStr := fmt.Sprintf("%s_%d", topic, partitionID)
	h := fnv.New32a()
	h.Write([]byte(TopicPartitionStr))
	return h.Sum32()
}

// Replication.

func contains(rs []int32, r int32) bool {
	for _, ri := range rs {
		if ri == r {
			return true
		}
	}
	return false
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Atomically sets a readiness state flag when leadership is obtained, to indicate that server is past its barrier write
func (b *Node) setConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (b *Node) resetConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (b *Node) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&b.readyForConsistentReads) == 1
}

func (b *Node) numPeers() (int, error) {
	future := b.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	raftConfig := future.Configuration()
	var numPeers int
	for _, server := range raftConfig.Servers {
		if server.Suffrage == raft.Voter {
			numPeers++
		}
	}
	return numPeers, nil
}

func (b *Node) LANMembers() []serf.Member {
	return b.serf.Members()
}

func (b *Node) withTimeout(timeout time.Duration,
	fn func() error) error {
	if timeout <= 0 {
		go fn()
		return nil
	}

	c := make(chan error, 1)
	defer close(c)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		c <- fn()
	}()

	select {
	case err := <-c:
		return err
	case <-timer.C:
		return errors.New("request timed out")
	}
}

func (b *Node) logState() {
	t := time.NewTicker(b.logStateInterval)
	for {
		select {
		case <-b.shutdownCh:
			return
		case <-t.C:
			var buf bytes.Buffer
			buf.WriteString("\tmembers:\n")
			members := b.LANMembers()
			for i, member := range members {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tname: %s\n\t\t\taddr: %s\n\t\t\tstatus: %s\n", i, member.Name, member.Addr, member.Status))
			}
			buf.WriteString("\tnodes:\n")
			state := b.fsm.State()
			_, nodes, err := state.GetNodes()
			if err != nil {
				panic(err)
			}
			for i, node := range nodes {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %d\n\t\t\tstatus: %s\n", i, node.Node, node.Check.Status))
			}
			_, topics, err := state.GetTopics()
			if err != nil {
				panic(err)
			}
			buf.WriteString("\ttopics:\n")
			for i, topic := range topics {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %s\n\t\t\tpartitions: %v\n", i, topic.Topic, topic.Partitions))
			}
			log.Info.Printf("state:\n%s", buf.String())
		}
	}
}
