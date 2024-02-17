package replica

import (
	"fmt"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

type CommitLogFSM struct {
	Log              CommitLog
	PartitionID      uint64
	RecordOffset     uint64
	LastAppliedIndex uint64
}

// CommitLogFSM creates and return a new CommitLogFSM object.
func NewCommitLogFSM(topicName string, commitLog CommitLog, shardID uint64, nodeID uint64) sm.IOnDiskStateMachine {

	return &CommitLogFSM{
		Log:              commitLog,
		PartitionID:      shardID,
		RecordOffset:     0,
		LastAppliedIndex: 0,
	}
}

// Lookup performs local lookup on the ExampleStateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (c *CommitLogFSM) Lookup(query interface{}) (interface{}, error) {
	/*
		result := make([]byte, 8)
		binary.LittleEndian.PutUint64(result, s.Count)
	*/
	return nil, nil
}

// Update updates the state machine.

func (c *CommitLogFSM) Update(ents []sm.Entry) ([]sm.Entry, error) {
	// Add Saftey against Raftlog Replay

	for idx, e := range ents {
		// Each entry is a RaftLog Entry
		/*
			var records []*grpc.DataRecord
			gob.NewDecoder(bytes.NewReader(e.Cmd)).Decode(&records)
			c.Log.AppendRecordSet(records)
		*/

		c.Log.AppendRecordSetBytes(e.Cmd)
		ents[idx].Result = sm.Result{Value: uint64(c.RecordOffset)}
	}

	return ents, nil
}

// Sync synchronizes all in-core state of the state machine. Since the Update
// method in this example already does that every time when it is invoked, the
// Sync method here is a NoOP.
func (c *CommitLogFSM) Sync() error {
	fmt.Println("Sync FSM at : ")
	return nil
}

func (c *CommitLogFSM) PrepareSnapshot() (interface{}, error) {
	fmt.Println("PrepareSnapshot FSM at : ")
	return nil, nil
}

// Open opens the state machine and return the index of the last Raft Log entry
// already updated into the state machine.
func (c *CommitLogFSM) Open(stopc <-chan struct{}) (uint64, error) {
	c.Log.NewestOffset()
	return uint64(c.Log.NewestOffset()), nil
}

// SaveSnapshot saves the state machine state identified by the state
// identifier provided by the input ctx parameter. Note that SaveSnapshot
// is not suppose to save the latest state.
func (d *CommitLogFSM) SaveSnapshot(ctx interface{},
	w io.Writer, done <-chan struct{}) error {
	fmt.Println("SaveSnapshot FSM at : ")
	return nil
}

// RecoverFromSnapshot recovers the state machine state from snapshot. The
// snapshot is recovered into a new DB first and then atomically swapped with
// the existing DB to complete the recovery.
func (c *CommitLogFSM) RecoverFromSnapshot(r io.Reader,
	done <-chan struct{}) error {
	fmt.Println("RecoverSnapshot FSM at : ")
	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (c *CommitLogFSM) Close() error {
	fmt.Println("Close FSM at : ")
	return nil
}
