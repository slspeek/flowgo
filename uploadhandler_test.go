package flow

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/slspeek/goblob"
	"io"
	"labix.org/v2/mgo"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

const cid = 42
const hex = "ABCD"

var f = Flow{"flow_id", "test.txt", 2, 1024}

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func blobService() (bs *goblob.BlobService) {
	s, err := mgo.Dial("localhost")
	if err != nil {
		panic(err)
	}
	bs = goblob.NewBlobService(s, "test", "testfs")
	return
}

func uploadHandler(f func(*http.Request, string)) *UploadHandler {
	return NewUploadHandler(blobService(), f)
}

func TestPutChunk(t *testing.T) {
	upload := newUpload(f)
	upload.put(cid, hex)
}

func TestGetChunk(t *testing.T) {
	upload := newUpload(f)
	upload.put(cid, hex)
	if id, _ := upload.get(cid); hex != id {
		t.Fail()
	}
}

func testBytes(n int) (buf *bytes.Buffer, md5sum string) {
	buf = new(bytes.Buffer)
	h := md5.New()
	for i := 0; i < n; i++ {
		c := byte(rand.Int())
		buf.WriteByte(c)
		h.Write([]byte{c})
	}
	md5sum = fmt.Sprintf("%x", h.Sum(nil))
	return
}

func PrepareRequests(url string, fn string, fs int64, chunkSize int64) (f Flow, reqs []*http.Request, md5sum string) {
	identifier := fmt.Sprintf("%d-%s", fs, fn)
	totalChunks := int(math.Ceil(float64(fs) / float64(chunkSize)))
	f = Flow{identifier, fn, totalChunks, chunkSize}
	reqs = make([]*http.Request, totalChunks)
	data, md5sum := testBytes(100*1024 - 2)
	buf := new(bytes.Buffer)
	for i := 1; i <= totalChunks; i++ {
		io.CopyN(buf, data, chunkSize)
		r := makeRequest(url, buf, f, i)
		reqs[i-1] = r
	}
	return
}

func TestWithTestServerMulti(t *testing.T) {
	reader, md5sum := testBytes(100*1024 - 2)
	fid := ""
	finished := make(chan bool, 1)
	ulh := uploadHandler(func(r *http.Request, id string) {
		fid = id
		finished <- true

	})
	f := Flow{"10-testparts", "random-10-part", 10, 10 * 1024}
	ts := httptest.NewServer(ulh)
	defer ts.Close()
	r := new(bytes.Buffer)
	for i := 1; i <= 10; i++ {
    log.Println("***************Befire request: ", i)
		io.CopyN(r, reader, 1024*1024)
		req := makeRequest(ts.URL, r, f, i)
		resp, _ := http.DefaultClient.Do(req)
		io.Copy(os.Stderr, resp.Body)
		if resp.StatusCode != 200 {
			t.Fatal("StatusCode should be 200")
		}
    log.Println("***************After request: ", i)
	}
	bs := blobService()
	defer bs.Close()
  log.Println("Waiting to finish")
	<-finished
  log.Println("finished!")
	file, err := bs.Open(fid)
	if err != nil {
		t.Fatal("Open file went south: ", err)
	}
	if file.MD5() != md5sum {
		t.Fatal("Checksum of uploaded file mismatched")
	}
	err = bs.Remove(fid)
	check(t, err)
}

func BenchmarkSequentialUpload(b *testing.B) {
	b.StopTimer()
	bs := blobService()
	defer bs.Close()
	fid := ""
	finished := make(chan bool, 1)
	ulh := uploadHandler(func(r *http.Request, id string) {
		fid = id
		finished <- true
	})
	ts := httptest.NewServer(ulh)
	defer ts.Close()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		fl, requests, md5sum := PrepareRequests(ts.URL, "foo.data", 100*1024-4, 1024)
		b.StartTimer()
		for i := 0; i < fl.TotalChunks; i++ {
			resp, err := http.DefaultClient.Do(requests[i])
			if err != nil {
				b.Fatal("DefaultClient recieves error: ", err)
			}
			if resp.StatusCode != 200 {
				b.Fatal("StatusCode should be 200")
			}
		}
		<-finished
		b.StopTimer()
		file, err := bs.Open(fid)
		if err != nil {
			b.Fatal("Open file went south: ", err)
		}
		if file.MD5() != md5sum {
			b.Fatal("Checksum of uploaded file mismatched")
		}
		bs.Remove(fid)
		b.StartTimer()
	}
}
func TestWithTestServerMultiClient(t *testing.T) {
	reader, md5sum := testBytes(100*1024 - 2)
	fid := ""
	finished := make(chan bool, 1)
	ulh := uploadHandler(func(r *http.Request, id string) {
		fid = id
		finished <- true

	})
	ts := httptest.NewServer(ulh)
	defer ts.Close()

	client := NewClient(ts.URL)

  client.Opts.ChunkSize = 1024
	client.Upload("foo.data", bytes.NewReader(reader.Bytes()))
	bs := blobService()
	defer bs.Close()
	<-finished
	file, err := bs.Open(fid)
	if err != nil {
		t.Fatal("Open file went south: ", err)
	}
	if file.MD5() != md5sum {
		t.Fatal("Checksum of uploaded file mismatched")
	}
	err = bs.Remove(fid)
	check(t, err)
}

func BenchmarkSequentialUploadClient(b *testing.B) {
	b.StopTimer()
	bs := blobService()
	defer bs.Close()
	fid := ""
	finished := make(chan bool, 1)
	ulh := uploadHandler(func(r *http.Request, id string) {
		fid = id
		finished <- true
	})
	ts := httptest.NewServer(ulh)
	defer ts.Close()
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		input, md5sum := testBytes(100*1024 - 4)

		client := NewClient(ts.URL)
    client.Opts.ChunkSize = 1024
    b.StartTimer()

		client.Upload("foo.data", bytes.NewReader(input.Bytes()))

		b.StartTimer()
		<-finished
		b.StopTimer()
		file, err := bs.Open(fid)
		if err != nil {
			b.Fatal("Open file went south: ", err)
		}
		if file.MD5() != md5sum {
			b.Fatal("Checksum of uploaded file mismatched")
		}
		bs.Remove(fid)
		b.StartTimer()
	}
}
