package commitlog

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"github.com/psridhark/goatq/core/util"
	"github.com/psridhark/goatq/grpc"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
}

type LogReader struct {
	cl  *CommitLog
	idx int
	mu  sync.Mutex
	pos int64
}

func (r *Reader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]

	var readSize int
	for {
		fmt.Println("READING IN : ", n)
		readSize, err = segment.ReadAt(p[n:], r.pos)
		n += readSize
		r.pos += int64(readSize)
		if readSize != 0 && err == nil {
			continue
		}
		if n == len(p) || err != io.EOF {
			break
		}
		if len(segments) <= r.idx+1 {
			err = io.EOF
			break
		}
		r.idx++
		segment = segments[r.idx]
		r.pos = 0
	}
	fmt.Println("TOTAL : ", string(p[n:]))
	return n, err
}

func (l *CommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	var s *Segment
	var idx int
	if offset == 0 {
		// TODO: seems hackish, should at least check if segments are set.
		s, idx = l.Segments()[0], 0
	} else {
		s, idx = findSegment(l.Segments(), offset)
	}
	if s == nil {
		return nil, errors.Wrapf(ErrSegmentNotFound, "segments: %d, offset: %d", len(l.Segments()), offset)
	}
	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &Reader{
		cl:  l,
		idx: idx,
		pos: e.Position,
	}, nil
}

func (l *CommitLog) NewLogReader(offset int64) (*LogReader, error) {
	var s *Segment
	var idx int
	if offset == 0 {
		// TODO: seems hackish, should at least check if segments are set.
		s, idx = l.Segments()[0], 0
	} else {
		s, idx = findSegment(l.Segments(), offset)
	}

	if s == nil {
		return nil, errors.Wrapf(ErrSegmentNotFound, "segments: %d, offset: %d", len(l.Segments()), offset)
	}

	e, err := s.findEntry(offset)
	if err != nil {
		return nil, err
	}
	return &LogReader{
		cl:  l,
		idx: idx,
		pos: e.Position,
	}, nil
}

func (r *LogReader) ReadRecord() (record Record, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]
	sizePos := 8
	msgSetHeaderLenT := 12
	var readSize int

	var dataBuf []byte

	ms := make([]byte, msgSetHeaderLenT)
	readSize, err = segment.ReadAt(ms, r.pos)
	r.pos += int64(readSize)
	size := Encoding.Uint32(ms[sizePos : sizePos+4])
	dataBuf = make([]byte, size)
	_, err = segment.ReadAt(dataBuf, r.pos)
	var res Record
	r.pos += int64(size)
	if size == 0 {
		if (r.idx + 1) < len(segments) {
			// new segment is there and size is 0
			return res, ErrEndOfSegment
		}
		return res, ErrNoMoreRecords
	}

	// Decode the JSON bytes into the struct
	if err = json.Unmarshal(dataBuf, &res); err != nil {
		return res, err
	}
	return res, err
}

func (r *LogReader) ReadRecordProto() (record *grpc.DataRecord, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]
	sizePos := 8
	msgSetHeaderLenT := 12
	var readSize int

	var dataBuf []byte

	ms := make([]byte, msgSetHeaderLenT)
	readSize, err = segment.ReadAt(ms, r.pos)
	r.pos += int64(readSize)
	size := Encoding.Uint32(ms[sizePos : sizePos+4])
	dataBuf = make([]byte, size)
	_, err = segment.ReadAt(dataBuf, r.pos)
	res := &grpc.DataRecord{}
	r.pos += int64(size)
	if size == 0 {
		if (r.idx + 1) < len(segments) {
			// new segment is there and size is 0
			return res, ErrEndOfSegment
		}
		return res, ErrNoMoreRecords
	}

	//decompedBytes, err := util.SnappyDecompress(dataBuf)

	decompedBytes, err := r.cl.compressor.Decompress(dataBuf)

	if err != nil {
		fmt.Println("YO DECOMPRESS ERROR : ", err)
		return res, err
	}
	// Decode the JSON bytes into the struct
	if err = proto.Unmarshal(decompedBytes, res); err != nil {
		return res, err
	}
	return res, err
}

func (r *LogReader) ReadRecordSet() (records []*grpc.DataRecord, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]
	sizePos := 8
	msgSetHeaderLenT := 12
	var readSize int

	var dataBuf []byte

	ms := make([]byte, msgSetHeaderLenT)
	readSize, err = segment.ReadAt(ms, r.pos)
	r.pos += int64(readSize)
	initialOffset := Encoding.Uint64(ms[0:sizePos])
	size := Encoding.Uint32(ms[sizePos : sizePos+4])
	dataBuf = make([]byte, size)
	_, err = segment.ReadAt(dataBuf, r.pos)

	r.pos += int64(size)
	if size == 0 {
		if (r.idx + 1) < len(segments) {
			// new segment is there and size is 0
			return nil, ErrEndOfSegment
		}
		return nil, ErrNoMoreRecords
	}
	// Read from bytes
	fmt.Println("THE INITIAL OFFSET : ", initialOffset)
	decompedRecordSetBytes, err := util.SnappyDecompress(dataBuf)
	if err != nil {
		fmt.Println("YO DECOMPRESS ERROR : ", err)
		return nil, err
	}

	res, err := r.DecodeRecordSet(decompedRecordSetBytes, initialOffset)
	if err != nil {
		return res, err
	}
	return res, err
}

func (r *LogReader) DecodeRecordSet(recordSetBytes []byte, initialOffset uint64) (records []*grpc.DataRecord, err error) {
	//records := make(grpc.DataRecord, 0)
	var protoRecords []*grpc.DataRecord

	sizePos := 8

	ind := 0

	for ind < len(recordSetBytes) {
		recordProto := &grpc.DataRecord{}
		sizeInd := ind + sizePos
		sizeIndEnd := sizeInd + 4
		recordSize := Encoding.Uint32(recordSetBytes[sizeInd:sizeIndEnd])
		recordBytes := recordSetBytes[sizeIndEnd : sizeIndEnd+int(recordSize)]

		if err = proto.Unmarshal(recordBytes, recordProto); err != nil {
			fmt.Println("ERROR IN UNMARSHAL : ", err)
			return nil, err
		}

		recordProto.Offset = initialOffset
		protoRecords = append(protoRecords, recordProto)
		ind += +8 + 4 + int(recordSize)
		initialOffset += 1

	}

	return protoRecords, err

}

func (r *LogReader) ReadRecordBytes() (record []byte, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	segments := r.cl.Segments()
	segment := segments[r.idx]
	sizePos := 8
	msgSetHeaderLenT := 12
	var readSize int

	var recordBytes []byte

	ms := make([]byte, msgSetHeaderLenT)
	readSize, err = segment.ReadAt(ms, r.pos)
	r.pos += int64(readSize)
	size := Encoding.Uint32(ms[sizePos : sizePos+4])
	recordBytes = make([]byte, size)
	_, err = segment.ReadAt(recordBytes, r.pos)

	r.pos += int64(size)
	if size == 0 {
		if (r.idx + 1) < len(segments) {
			// new segment is there and size is 0
			return recordBytes, ErrEndOfSegment
		}
		return recordBytes, ErrNoMoreRecords
	}

	return recordBytes, err
}
