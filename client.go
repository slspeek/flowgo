package flow

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
)

type Opts struct {
	ChunkSize          int64
	FileParameterName  string
	Headers            map[string]string
	Target             string
	MaxChunkRetries    int
	ChunkRetryInterval int
	PermanentErrors    []int
}

type Client struct {
	Opts Opts
}

func NewClient(target string) *Client {
	client := new(Client)
	opts := Opts{ChunkSize: 1024 * 1024,
		FileParameterName:  "file",
		Headers:            map[string]string{},
		Target:             target,
		MaxChunkRetries:    0,
		ChunkRetryInterval: 3000,
		PermanentErrors:    []int{401, 404, 500},
	}
	client.Opts = opts
	return client
}

func makeRequest(url string, body io.Reader, f Flow, chunkNumber int) *http.Request {
	buf := new(bytes.Buffer)
	writer := multipart.NewWriter(buf)
	writer.WriteField("flowChunkSize", fmt.Sprintf("%v", f.ChunkSize))
	writer.WriteField("flowChunkNumber", fmt.Sprintf("%v", chunkNumber))
	writer.WriteField("flowTotalChunks", fmt.Sprintf("%v", f.TotalChunks))
	writer.WriteField("flowIdentifier", f.Identifier)
	writer.WriteField("flowFilename", f.Filename)
	fileWriter, _ := writer.CreateFormFile("file", f.Filename)
	io.Copy(fileWriter, body)
	writer.Close()
	req, _ := http.NewRequest("POST", url, buf)
	req.Header.Add("Content-Type", writer.FormDataContentType())
	return req
}

func (c *Client) UploadFile(fn string) (err error) {
  var input io.ReadSeeker
  input, err = os.Open(fn)
  return c.Upload(fn, input)
}

func (c *Client) Upload(fn string, input io.ReadSeeker) (err error) {
	fs, err := input.Seek(0, 2)
	if err != nil {
		return
	}
	_, err = input.Seek(0, 0)
	if err != nil {
		return
	}

	identifier := fmt.Sprintf("%d-%s", fs, fn)
	totalChunks := int(math.Ceil(float64(fs) / float64(c.Opts.ChunkSize)))
	f := Flow{identifier, fn, totalChunks, c.Opts.ChunkSize}

	for i := 1; i <= totalChunks; i++ {
		buf := new(bytes.Buffer)
		io.CopyN(buf, input, c.Opts.ChunkSize)
		r := makeRequest(c.Opts.Target, buf, f, i)
    for key, value := range(c.Opts.Headers) {
      r.Header.Add(key, value)
    }
		var resp *http.Response
		resp, err = http.DefaultClient.Do(r)
		if err != nil {
			return
		}
		for _, code := range c.Opts.PermanentErrors {
			if resp.StatusCode == code {
        return errors.New(fmt.Sprintf("Permanent error occured: %d",code))
			}
		}
	}
	return
}
