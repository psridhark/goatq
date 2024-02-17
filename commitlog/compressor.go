package commitlog

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"github.com/golang/snappy"
)

type Compressor interface {
	Compress([]byte) []byte
	Decompress([]byte) ([]byte, error)
}

// The compressor for compressing and decompressing the recordSet

type SnappyCompressor struct {
}

type GzipCompressor struct {
}

type NoCompressor struct {
}

func NewSnappyCompressor() *SnappyCompressor {
	c := &SnappyCompressor{}
	return c
}

func (c *SnappyCompressor) Compress(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func (c *SnappyCompressor) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func NewGzipCompressor() *GzipCompressor {
	c := &GzipCompressor{}
	return c
}

func (c *GzipCompressor) Compress(s []byte) []byte {

	zipbuf := bytes.Buffer{}
	zipped := gzip.NewWriter(&zipbuf)
	zipped.Write(s)
	zipped.Close()
	//fmt.Println("compressed size (bytes): ", len(zipbuf.Bytes()))
	return zipbuf.Bytes()
}

func (c *GzipCompressor) Decompress(s []byte) ([]byte, error) {

	rdr, _ := gzip.NewReader(bytes.NewReader(s))
	defer rdr.Close()
	return ioutil.ReadAll(rdr)
}

func NewNoCompressor() *NoCompressor {
	c := &NoCompressor{}
	return c
}

func (c *NoCompressor) Compress(data []byte) []byte {
	return data
}

func (c *NoCompressor) Decompress(data []byte) ([]byte, error) {
	return data, nil
}
