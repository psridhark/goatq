package fsm

import (
	"fmt"

	"github.com/psridhark/goatq/core/replica"
	"github.com/psridhark/goatq/core/structs"
	"github.com/psridhark/goatq/log"
)

func init() {
	registerCommand(structs.RegisterNodeRequestType, (*FSM).applyRegisterNode)
	registerCommand(structs.DeregisterNodeRequestType, (*FSM).applyDeregisterNode)
	registerCommand(structs.RegisterTopicRequestType, (*FSM).applyRegisterTopic)
	registerCommand(structs.DeregisterTopicRequestType, (*FSM).applyDeregisterTopic)
	registerCommand(structs.RegisterPartitionRequestType, (*FSM).applyRegisterPartition)
	registerCommand(structs.DeregisterPartitionRequestType, (*FSM).applyDeregisterPartition)
	registerCommand(structs.RegisterGroupRequestType, (*FSM).applyRegisterGroup)
}

func (c *FSM) applyRegisterGroup(buf []byte, index uint64) interface{} {
	var req structs.RegisterGroupRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.EnsureGroup(index, &req.Group); err != nil {
		log.Error.Printf("EnsureNode error: %s", err)
		return err
	}

	return nil
}

func (c *FSM) applyRegisterNode(buf []byte, index uint64) interface{} {
	var req structs.RegisterNodeRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.EnsureNode(index, &req.Node); err != nil {
		log.Error.Printf("EnsureNode error: %s", err)
		return err
	}

	return nil
}

func (c *FSM) applyDeregisterNode(buf []byte, index uint64) interface{} {
	var req structs.DeregisterNodeRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.DeleteNode(index, req.Node.Node); err != nil {
		log.Error.Printf("DeleteNode error: %s", err)
		return err
	}

	return nil
}

func (c *FSM) applyRegisterTopic(buf []byte, index uint64) interface{} {
	var req structs.RegisterTopicRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.EnsureTopic(index, &req.Topic); err != nil {
		log.Error.Printf("EnsureTopic error: %s", err)
		return err
	}

	return nil
}

func (c *FSM) applyDeregisterTopic(buf []byte, index uint64) interface{} {
	var req structs.DeregisterTopicRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.DeleteTopic(index, req.Topic.Topic); err != nil {
		log.Error.Printf("DeleteTopic error: %s", err)
		return err
	}

	return nil
}

func contains(ids []int32, id NodeID) bool {
	for _, idd := range ids {
		if idd == int32(id) {
			return true
		}
	}
	return false
}

func (c *FSM) applyRegisterPartition(buf []byte, index uint64) interface{} {
	var req structs.RegisterPartitionRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.EnsurePartition(index, &req.Partition); err != nil {
		log.Error.Printf("EnsurePartition error: %s", err)
		return err
	}

	p := req.Partition
	if contains(p.AR, c.nodeID) {

		// We shall setup the raftGroupID while setting up Raft
		repl := &replica.Replica{
			BrokerID: int32(c.nodeID),
			Partition: structs.Partition{
				ID:              p.Partition,
				Partition:       p.Partition,
				RaftGroupID:     p.RaftGroupID,
				Topic:           p.Topic,
				ISR:             p.ISR,
				AR:              p.AR,
				ControllerEpoch: p.ControllerEpoch,
				LeaderEpoch:     p.LeaderEpoch,
				Leader:          p.Leader,
			},
			IsLocal: true,
		}
		// send to the replicaCh to process the raftGroup
		c.ReplicaCh <- repl
	}

	return nil
}

func (c *FSM) applyDeregisterPartition(buf []byte, index uint64) interface{} {
	var req structs.DeregisterPartitionRequest
	if err := structs.Decode(buf, &req); err != nil {
		panic(fmt.Errorf("failed to decode request: %v", err))
	}

	if err := c.state.DeletePartition(index, req.Partition.Topic, req.Partition.Partition); err != nil {
		log.Error.Printf("DeletePartition error: %s", err)
		return err
	}

	return nil
}
