package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/cespare/xxhash"
	"github.com/golang/snappy"
)

func Dump(i interface{}) string {
	return fmt.Sprintf("%s", i)
	// return strings.Replace(spew.Sdump(i), "\n", "", -1)
}

func Hash(s string) uint64 {
	h := xxhash.New()
	if _, err := h.Write([]byte(s)); err != nil {
		panic(err)
	}
	return h.Sum64()
}

func SnappyCompress(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func SnappyDecompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)

}

func GzipCompress(s []byte) []byte {

	zipbuf := bytes.Buffer{}
	zipped := gzip.NewWriter(&zipbuf)
	zipped.Write(s)
	zipped.Close()
	//fmt.Println("compressed size (bytes): ", len(zipbuf.Bytes()))
	return zipbuf.Bytes()
}

func GzipDecompress(s []byte) []byte {

	rdr, _ := gzip.NewReader(bytes.NewReader(s))
	data, err := ioutil.ReadAll(rdr)
	if err != nil {
		log.Fatal(err)
	}
	rdr.Close()
	//fmt.Println("uncompressed size (bytes): ", len(data))
	return data
}
