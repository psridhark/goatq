package replica

import (
	"fmt"
	"sync"

	"github.com/psridhark/goatq/core/structs"
)

type Replica struct {
	BrokerID  int32
	Partition structs.Partition
	IsLocal   bool
	Log       CommitLog
	Hw        int64
	sync.Mutex
}

func (r Replica) String() string {
	return fmt.Sprintf("replica: %d {broker: %d, leader: %d}", r.Partition.ID, r.BrokerID, r.Partition.Leader)
}

type ReplicaLookup struct {
	lock sync.RWMutex
	// topic to partition id to replica id to replica
	replica map[string]map[int32]*Replica
}

func NewReplicaLookup() *ReplicaLookup {
	return &ReplicaLookup{
		replica: make(map[string]map[int32]*Replica),
	}
}

func (rl *ReplicaLookup) AddReplica(replica *Replica) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
ADD:
	if t, ok := rl.replica[replica.Partition.Topic]; ok {
		t[replica.Partition.ID] = replica
	} else {
		rl.replica[replica.Partition.Topic] = make(map[int32]*Replica)
		goto ADD
	}
}

func (rl *ReplicaLookup) Replica(topic string, partition int32) (*Replica, error) {
	rl.lock.RLock()
	defer rl.lock.RUnlock()
	r, ok := rl.replica[topic][partition]
	if !ok {
		return nil, fmt.Errorf("no replica for topic %s partition %d", topic, partition)
	}
	return r, nil
}

func (rl *ReplicaLookup) RemoveReplica(replica *Replica) {
	rl.lock.Lock()
	defer rl.lock.Unlock()
	delete(rl.replica[replica.Partition.Topic], replica.Partition.ID)
}
