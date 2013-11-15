package flow

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"github.com/slspeek/goblob"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

const cid = 42
const hex = "ABCD"

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func blobService() (bs *goblob.BlobService) {
	bs, _ = goblob.NewBlobService("localhost", "test", "testfs")
  return
}

func uploadHandler(t *testing.T, f func(*http.Request, string)) *UploadHandler {
	return NewUploadHandler(blobService(), f)
}

func makeRequest(t *testing.T, url string, body io.Reader, f flow, chunkNumber int) *http.Request {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	writer.WriteField("flowChunkSize", fmt.Sprintf("%v", f.chunkSize))
	writer.WriteField("flowChunkNumber", fmt.Sprintf("%v", chunkNumber))
	writer.WriteField("flowTotalChunks", fmt.Sprintf("%v", f.totalChunks))
	writer.WriteField("flowIdentifier", f.identifier)
	writer.WriteField("flowFilename", f.filename)
	fileWriter, err := writer.CreateFormFile("file", f.filename)
	check(t, err)
	_, err = io.Copy(fileWriter, body)
	check(t, err)
	writer.Close()
	req, err := http.NewRequest("POST", url, buf)
	check(t, err)
	req.Header.Add("Content-Type", writer.FormDataContentType())
	return req
}

func TestPutChunk(t *testing.T) {
	upload := newUpload("/etc/gotube", 10)
	upload.put(cid, hex)
}

func TestGetChunk(t *testing.T) {
	upload := newUpload("settings.xml", 2)
	upload.put(cid, hex)
	if id, _ := upload.get(cid); hex != id {
		t.Fail()
	}
}

func TestWithTestServer(t *testing.T) {
  fid := ""
	ulh := uploadHandler(t, func(r *http.Request, id string){
    fid = id
  })
	upload := ulh.uploads.get("123-passwd", "passwd", 1)
	ts := httptest.NewServer(ulh)
	defer ts.Close()
	reader, md5sum := testBytes(100)
	f := flow{"123-passwd", "passwd", 1, 1024 * 1024}
	req := makeRequest(t, ts.URL, reader, f, 1)
	resp, _ := http.DefaultClient.Do(req)
	io.Copy(os.Stderr, resp.Body)
	if resp.StatusCode != 200 {
		t.Fatal("StatusCode should be 200")
	}
	t.Log("Look for md5sum: ", md5sum)
	<-upload.finished
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

func TestWithTestServerMulti(t *testing.T) {
	reader, md5sum := testBytes(10 * 1024 * 1024)
  fid := ""
	ulh := uploadHandler(t, func(r *http.Request, id string){
    fid = id
  })
	upload := ulh.uploads.get("10-testparts", "random-10-part", 10)
	ts := httptest.NewServer(ulh)
	defer ts.Close()
  r := new(bytes.Buffer)
	f := flow{"10-testparts", "random-10-part", 10, 1024 * 1024}
	for i := 1; i <= 10; i++ {
    io.CopyN(r, reader, 1024*1024)
		req := makeRequest(t, ts.URL, r, f, i)
		resp, _ := http.DefaultClient.Do(req)
		io.Copy(os.Stderr, resp.Body)
		if resp.StatusCode != 200 {
			t.Fatal("StatusCode should be 200")
		}
	}
	<-upload.finished
  bs := blobService()
  t.Log("File id: ", fid)
  file, err := bs.Open(fid)
  if err != nil {
    t.Fatal("Open file went south: ", err) 
  }
  if file.MD5() != md5sum {
    t.Fatal("Checksum of uploaded file mismatched")
  }
}
