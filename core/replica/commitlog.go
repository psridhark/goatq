package replica

import (
	"io"

	"github.com/psridhark/goatq/commitlog"
	"github.com/psridhark/goatq/grpc"
)

type CommitLog interface {
	Delete() error
	NewReader(offset int64, maxBytes int32) (io.Reader, error)
	NewLogReader(offset int64) (*commitlog.LogReader, error)
	Truncate(int64) error
	NewestOffset() int64
	OldestOffset() int64
	Append([]byte) (int64, error)
	AppendRecordSetBytes(recordSetBytes []byte) (int64, error)
	EncodeRecordSet(initalOffset uint64, record []*grpc.DataRecord) ([]byte, error)
}
