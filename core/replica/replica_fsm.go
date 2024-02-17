package replica

import (
	"bytes"
	"encoding/gob"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/psridhark/goatq/grpc"
	"github.com/psridhark/goatq/log"
)

type ReplicaFSM struct {
	RecordOffset     uint64
	Log              CommitLog
	TopicName        string
	NodeID           int32
	PartitionID      uint64
	Offset           uint64
	LastAppliedIndex uint64
}

// ReplicaFSM creates and return a new ReplicaFSM object.
func NewReplicaFSM(topicName string, commitLog CommitLog, shardID uint64, nodeID uint64) sm.IStateMachine {

	return &ReplicaFSM{
		Log:              commitLog,
		PartitionID:      shardID,
		RecordOffset:     0,
		LastAppliedIndex: 0,
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *ReplicaFSM) Lookup(query interface{}) (interface{}, error) {
	/*
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, s.Count)
	*/
	return nil, nil
}

// Update updates the object using the specified committed raft entry.
func (s *ReplicaFSM) Update(e sm.Entry) (sm.Result, error) {
	// in this example, we print out the following message for each
	// incoming update request. we also increase the counter by one to remember
	// how many updates we have applied

	// Each entry is a RaftLog Entry

	var records []*grpc.DataRecord

	gob.NewDecoder(bytes.NewReader(e.Cmd)).Decode(&records)
	recordSetBytes, err := s.Log.EncodeRecordSet(0, records)
	if err != nil {
		log.Debug.Printf("Error in commiting :%v\n", err)
	}
	s.Log.AppendRecordSetBytes(recordSetBytes)

	// incorrect
	return sm.Result{Value: s.Offset}, nil
}

// Open opens the state machine and return the index of the last Raft Log entry
// already updated into the state machine.
func (s *ReplicaFSM) Open(stopc <-chan struct{}) (uint64, error) {
	return uint64(1), nil
}

// SaveSnapshot saves the current IStateMachine state into a snapshot using the
// specified io.Writer object.
func (s *ReplicaFSM) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	// as shown above, the only state that can be saved is the Count variable
	// there is no external file in this IStateMachine example, we thus leave
	// the fc untouched

	return nil
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *ReplicaFSM) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	// restore the Count variable, that is the only state we maintain in this
	// example, the input files is expected to be empty

	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *ReplicaFSM) Close() error { return nil }
