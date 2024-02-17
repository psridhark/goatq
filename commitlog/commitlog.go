package commitlog

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/psridhark/goatq/core/util"
	"github.com/psridhark/goatq/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	ErrSegmentNotFound = errors.New("segment not found")

	ErrEndOfSegment = errors.New("End of Segment")

	ErrNoMoreRecords = errors.New("No more records")

	Encoding = binary.BigEndian
)

type CleanupPolicy string

type CompressionType string

const (
	DeleteCleanupPolicy  = "delete"
	CompactCleanupPolicy = "compact"

	SnappyCompression = "snappy"
	GzipCompression   = "gzip"
	NoCompression     = "nocompression"

	LogFileSuffix   = ".log"
	IndexFileSuffix = ".index"
)

const (
	RecordHdrBytesLen    = 12
	RecordOffSetBytesLen = 8
	RecordSizeBytesLen   = 4
)

type CommitLog struct {
	Options
	cleaner        Cleaner
	compressor     Compressor
	name           string
	mu             sync.RWMutex
	segments       []*Segment
	vActiveSegment atomic.Value
}

type Options struct {
	Path string
	// MaxSegmentBytes is the max number of bytes a segment can contain, once the limit is hit a
	// new segment will be split off.
	MaxSegmentBytes int64
	MaxLogBytes     int64
	CleanupPolicy   CleanupPolicy
	CompressionType CompressionType
}

func New(opts Options) (*CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.MaxSegmentBytes == 0 {
		// TODO default here
	}

	if opts.CleanupPolicy == "" {
		opts.CleanupPolicy = DeleteCleanupPolicy
	}

	var cleaner Cleaner
	if opts.CleanupPolicy == DeleteCleanupPolicy {
		cleaner = NewDeleteCleaner(opts.MaxLogBytes)
	} else {
		cleaner = NewCompactCleaner()
	}

	var compressor Compressor
	if opts.CompressionType == SnappyCompression {
		compressor = NewSnappyCompressor()
	} else if opts.CompressionType == GzipCompression {
		compressor = NewGzipCompressor()
	} else {
		compressor = NewNoCompressor()
	}

	path, _ := filepath.Abs(opts.Path)
	l := &CommitLog{
		Options:    opts,
		name:       filepath.Base(path),
		cleaner:    cleaner,
		compressor: compressor,
	}

	if err := l.init(); err != nil {
		return nil, err
	}

	if err := l.open(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *CommitLog) init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *CommitLog) open() error {
	files, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	for _, file := range files {
		// if this file is an index file, make sure it has a corresponding .log file
		if strings.HasSuffix(file.Name(), IndexFileSuffix) {
			_, err := os.Stat(filepath.Join(l.Path, strings.Replace(file.Name(), IndexFileSuffix, LogFileSuffix, 1)))
			if os.IsNotExist(err) {
				if err := os.Remove(file.Name()); err != nil {
					return err
				}
			} else if err != nil {
				return errors.Wrap(err, "stat file failed")
			}
		} else if strings.HasSuffix(file.Name(), LogFileSuffix) {
			offsetStr := strings.TrimSuffix(file.Name(), LogFileSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return err
			}
			segment, err := NewSegment(l.Path, int64(baseOffset), l.MaxSegmentBytes)
			if err != nil {
				return err
			}
			fmt.Printf("LOAD THE SEGMENT : %s , Base : %v , Next :  %v\n", segment.path, segment.BaseOffset, segment.NextOffset)
			l.segments = append(l.segments, segment)
		}
	}
	if len(l.segments) == 0 {
		segment, err := NewSegment(l.Path, 0, l.MaxSegmentBytes)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	l.vActiveSegment.Store(l.segments[len(l.segments)-1])
	return nil
}

func (l *CommitLog) AppendRecordProto(record *grpc.DataRecord) (offset int64, err error) {

	recordHdr := make([]byte, RecordHdrBytesLen)

	if l.checkSplit() {
		if err := l.split(); err != nil {
			return offset, err
		}
	}
	position := l.activeSegment().Position
	offset = l.activeSegment().NextOffset

	// put the size in to size bytes
	record.Offset = uint64(offset)
	recordBytes, err := proto.Marshal(record)
	compressedBytes := util.SnappyCompress(recordBytes)
	Encoding.PutUint64(recordHdr[0:RecordOffSetBytesLen], uint64(offset))
	Encoding.PutUint32(recordHdr[RecordOffSetBytesLen:RecordHdrBytesLen], uint32(len(compressedBytes)))

	compressedBytes = append(recordHdr, compressedBytes...)
	if _, err := l.activeSegment().Write(compressedBytes); err != nil {
		return offset, err
	}
	e := Entry{
		Offset:   offset,
		Position: position,
	}
	if err := l.activeSegment().Index.WriteEntry(e); err != nil {
		return offset, err
	}
	return offset, nil
}

func (l *CommitLog) AppendRecordSetBytes(recordSetBytes []byte) (offset int64, err error) {

	/*
		The recordSetBytes is the encoded bytes of recordSet with the 4 byte recordSetHdr in the beginning.
	*/

	if l.checkSplit() {
		if err := l.split(); err != nil {
			return offset, err
		}
	}
	// Currently the write is sequenced via raft so we dont worry about locking these.
	initialPosition := l.activeSegment().Position
	initialOffset := l.activeSegment().NextOffset

	// Get the number of records in recordSet from the recordSetHdr bytes
	numRecords := Encoding.Uint32(recordSetBytes[0:4])

	// add a top level recordSetHdr to include the initial offset for this recordSet and also
	// total size of the encoded recordSet
	recordSetHdr := make([]byte, 12)
	Encoding.PutUint64(recordSetHdr[0:8], uint64(initialOffset))
	Encoding.PutUint32(recordSetHdr[8:12], uint32(len(recordSetBytes[4:])))
	recordSetBytesWithHdr := append(recordSetHdr, recordSetBytes[4:]...)

	if _, err := l.activeSegment().WriteRecordSet(recordSetBytesWithHdr, int(numRecords)); err != nil {

		return offset, err
	}

	tempOffset := initialOffset

	endOffset := initialOffset + int64(numRecords)
	for tempOffset < endOffset {
		e := Entry{
			Offset:   tempOffset,
			Position: initialPosition,
		}
		//fmt.Println("INDEXED HERE : ", tempOffset, initialPosition)
		if err := l.activeSegment().Index.WriteEntry(e); err != nil {
			return tempOffset, err
		}
		tempOffset += 1
	}

	// TODO : Optimize the indexing by having an entry at recordSet level and
	// do a binary search while looking up
	/*
		e := Entry{
			Offset:   tempOffset,
			Position: initialPosition,
		}
		//fmt.Println("INDEXED HERE : ", initialOffset, initialPosition)

		if err := l.activeSegment().Index.WriteEntry(e); err != nil {
			return tempOffset, err
		}

	*/
	return offset, nil
}

func (l *CommitLog) EncodeRecordSet(initialOffset uint64, records []*grpc.DataRecord) ([]byte, error) {

	/*
		Encode each record with a (offset,size,payload) bytes slice and append all of them and run
		them through the compressor if needed.
		A 4 byte header is added to the RecordSet which contains the number of records encoded.
	*/
	offset := initialOffset
	var recordSetBytes []byte
	numRecords := len(records)
	for _, protoRecord := range records {
		protoRecord.Offset = uint64(offset)
		recordHdr := make([]byte, RecordHdrBytesLen)
		// Put the offset into the header
		Encoding.PutUint64(recordHdr[0:8], uint64(offset))
		recordBytes, err := proto.Marshal(protoRecord)
		if err != nil {
			// TODO : Better error handling.
			fmt.Println("Record marshalling to bytes failed : ", err)
		}
		// Put the size of the record into header
		Encoding.PutUint32(recordHdr[8:12], uint32(len(recordBytes)))
		recordPairBytes := append(recordHdr, recordBytes...)
		recordSetBytes = append(recordSetBytes, recordPairBytes...)
		offset += 1
	}

	recordSetHdr := make([]byte, 4) // 4 byte header
	Encoding.PutUint32(recordSetHdr, uint32(numRecords))
	compressedRecordSet := l.compressor.Compress(recordSetBytes)
	compressedRecordSet = append(recordSetHdr, compressedRecordSet...)
	return compressedRecordSet, nil
}

func (l *CommitLog) Append(b []byte) (offset int64, err error) {
	ms := MessageSet(b)
	if l.checkSplit() {
		if err := l.split(); err != nil {
			return offset, err
		}
	}
	position := l.activeSegment().Position
	offset = l.activeSegment().NextOffset
	ms.PutOffset(offset)
	if _, err := l.activeSegment().Write(ms); err != nil {
		return offset, err
	}
	e := Entry{
		Offset:   offset,
		Position: position,
	}
	if err := l.activeSegment().Index.WriteEntry(e); err != nil {
		return offset, err
	}
	return offset, nil
}

func (l *CommitLog) Read(p []byte) (n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeSegment().Read(p)
}

func (l *CommitLog) NewestOffset() int64 {
	return l.activeSegment().NextOffset
}

func (l *CommitLog) OldestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].BaseOffset
}

func (l *CommitLog) activeSegment() *Segment {
	return l.vActiveSegment.Load().(*Segment)
}

func (l *CommitLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *CommitLog) Delete() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Path)
}

func (l *CommitLog) Truncate(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*Segment
	for _, segment := range l.segments {
		if segment.BaseOffset < offset {
			if err := segment.Delete(); err != nil {
				return err
			}
		} else {
			segments = append(segments, segment)
		}
	}
	l.segments = segments
	return nil
}

func (l *CommitLog) Segments() []*Segment {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.segments
}

func (l *CommitLog) checkSplit() bool {
	return l.activeSegment().IsFull()
}

func (l *CommitLog) split() error {
	segment, err := NewSegment(l.Path, l.NewestOffset(), l.MaxSegmentBytes)
	if err != nil {
		return err
	}
	l.mu.Lock()
	segments := append(l.segments, segment)
	segments, err = l.cleaner.Clean(segments)
	if err != nil {
		l.mu.Unlock()
		return err
	}
	l.segments = segments
	l.mu.Unlock()
	l.vActiveSegment.Store(segment)
	return nil
}
