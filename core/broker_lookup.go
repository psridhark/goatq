package core

import (
	"fmt"
	"math/rand"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/psridhark/goatq/core/metadata"
)

type BrokerLookup struct {
	lock            sync.RWMutex
	addressToBroker map[raft.ServerAddress]*metadata.Broker
	idToBroker      map[raft.ServerID]*metadata.Broker
}

func NewBrokerLookup() *BrokerLookup {
	// load from raft store if possible
	return &BrokerLookup{
		addressToBroker: make(map[raft.ServerAddress]*metadata.Broker),
		idToBroker:      make(map[raft.ServerID]*metadata.Broker),
	}
}

func (b *BrokerLookup) AddBroker(broker *metadata.Broker) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.addressToBroker[raft.ServerAddress(broker.RaftAddr)] = broker
	b.idToBroker[raft.ServerID(broker.ID.String())] = broker
}

func (b *BrokerLookup) BrokerByAddr(addr raft.ServerAddress) *metadata.Broker {
	b.lock.RLock()
	defer b.lock.RUnlock()
	svr, _ := b.addressToBroker[addr]
	return svr
}

func (b *BrokerLookup) BrokerByID(id raft.ServerID) *metadata.Broker {
	b.lock.RLock()
	defer b.lock.RUnlock()
	svr, _ := b.idToBroker[id]
	return svr
}

func (b *BrokerLookup) BrokerAddr(id raft.ServerID) (raft.ServerAddress, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	svr, ok := b.idToBroker[id]
	if !ok {
		return "", fmt.Errorf("no broker for id %v", id)
	}
	return raft.ServerAddress(svr.RaftAddr), nil
}

func (b *BrokerLookup) RemoveBroker(broker *metadata.Broker) {
	b.lock.Lock()
	defer b.lock.Unlock()
	delete(b.addressToBroker, raft.ServerAddress(broker.RaftAddr))
	delete(b.idToBroker, raft.ServerID(broker.ID.String()))
}

func (b *BrokerLookup) Brokers() []*metadata.Broker {
	b.lock.RLock()
	defer b.lock.RUnlock()
	ret := make([]*metadata.Broker, 0, len(b.addressToBroker))
	for _, svr := range b.addressToBroker {
		ret = append(ret, svr)
	}
	return ret
}

func (b *BrokerLookup) RandomBroker() *metadata.Broker {
	brokers := b.Brokers()
	i := rand.Intn(len(brokers))
	return brokers[i]
}
