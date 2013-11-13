package flow

import (
	"bytes"
	"fmt"
	"github.com/slspeek/goblob"
	"io"
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
		t.Error(err)
	}
}

func uploadHandler(t *testing.T) *UploadHandler {
	bs, err := goblob.NewBlobService("localhost", "test", "testfs")
	check(t, err)
	return NewUploadHandler(bs, func(r *http.Request, id string) {
		t.Log("Finished writing: ", id)
	})
}

func makeRequest(t *testing.T, url string, body io.Reader, flowChunkNumber int,
	flowTotalChunks int, flowFilename string, flowIdentifier string) *http.Request {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	writer.WriteField("flowChunkNumber", fmt.Sprintf("%v", flowChunkNumber))
	writer.WriteField("flowTotalChunks", fmt.Sprintf("%v", flowTotalChunks))
	writer.WriteField("flowIdentifier", flowIdentifier)
	writer.WriteField("flowFilename", flowFilename)
	fileWriter, err := writer.CreateFormFile("file", flowFilename)
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
	ulh := uploadHandler(t)
	ts := httptest.NewServer(ulh)
	defer ts.Close()
	reader, err := os.Open("/etc/passwd")
	check(t, err)
	req := makeRequest(t, ts.URL, reader, 1, 1, "passwd", "123-passwd")
	resp, err := http.DefaultClient.Do(req)
	io.Copy(os.Stderr, resp.Body)
	if resp.StatusCode != 200 {
		t.Fail()
	}
}
