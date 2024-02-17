package commitlog

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type Record struct {
	Key       string
	Value     string
	Timestamp int64
	Offset    int64
	RaftIndex uint64
}

type BatchRecord struct {
	Records []byte
}

func NewRecord(value string) Record {
	ts := time.Now().UnixMilli()
	return Record{Key: fmt.Sprintf("%s", ts), Value: value, Timestamp: ts}
}

func (r Record) encode() []byte {
	// protobuff encode ?

	encodedBytes, err := json.Marshal(r)
	if err != nil {
		log.Fatal(err)
	}
	return encodedBytes
}
