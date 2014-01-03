package flow

import (
	"fmt"
	"github.com/slspeek/goblob"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Flow struct {
	Identifier  string
	Filename    string
	TotalChunks int
	ChunkSize   int64
}

func readFlow(r *http.Request) (f Flow, chunkNumber int, err error) {
	identifier := r.FormValue("flowIdentifier")
	filename := r.FormValue("flowFilename")
	chunkNumber, err = strconv.Atoi(r.FormValue("flowChunkNumber"))
	if err != nil {
		return
	}
	totalChunks, err := strconv.Atoi(r.FormValue("flowTotalChunks"))
	if err != nil {
		return
	}
	chunkSize, err := strconv.Atoi(r.FormValue("flowChunkSize"))
	if err != nil {
		return
	}
	f = Flow{identifier, filename, totalChunks, int64(chunkSize)}
	return
}

type upload struct {
	mutex    *sync.Mutex
	chunks   map[int]string
	flow     Flow
	once     *sync.Once
	writeOut *writeOut
}

func newUpload(flow Flow) *upload {
	return &upload{mutex: new(sync.Mutex), chunks: make(map[int]string), flow: flow, once: new(sync.Once)}
}

func (self *upload) put(chunkId int, blobId string) {
	self.mutex.Lock()
	self.chunks[chunkId] = blobId
	self.mutex.Unlock()
}

func (self *upload) get(chunkId int) (string, bool) {
	self.mutex.Lock()
	result, existed := self.chunks[chunkId]
	self.mutex.Unlock()
	return result, existed
}

func (self *upload) getChunk(chunkId int, bs *goblob.BlobService) *goblob.File {
	self.mutex.Lock()
	id, _ := self.chunks[chunkId]
	self.mutex.Unlock()
	chunk, _ := bs.Open(id)
	return chunk
}

func (self *upload) hasAllChunks() bool {
	for i := 1; i <= self.flow.TotalChunks; i++ {
		if _, found := self.get(i); !found {
			return false
		}
	}
	return true
}

type writeOut struct {
	upload      *upload
	bs          *goblob.BlobService
	tickle      chan string
	result      chan bool
	outBlobId   string
	fileHandle  *goblob.File
	lastWritten int
	err         error
}

func startWrite(u *upload, bs *goblob.BlobService) (w *writeOut) {
	w = new(writeOut)
	w.tickle = make(chan string, u.flow.TotalChunks)
	w.result = make(chan bool)
	w.bs = bs
	w.upload = u
	file, err := bs.Create(u.flow.Filename)
	if err != nil {
		w.err = err
		return
	}
	w.fileHandle = file
	w.outBlobId = file.StringId()
	return
}

func (w *writeOut) writeOut() (finished bool) {
	var i int
	for i = 1 + w.lastWritten; i <= w.upload.flow.TotalChunks; i++ {
		if _, existed := w.upload.get(i); existed {
			chunk := w.upload.getChunk(i, w.bs)
			_, w.err = io.Copy(w.fileHandle, chunk)
			if w.err != nil {
				return
			}
			chunk.Close()
			w.bs.Remove(chunk.StringId())
		} else {
			break
		}
	}
	if i >= w.upload.flow.TotalChunks {
		finished = true
		w.err = w.fileHandle.Close()
	} else {
		w.lastWritten = i - 1
	}
	return
}

func (w *writeOut) waitForParts() {
	for {
		select {
		case <-w.tickle:
			if w.writeOut() {
				w.result <- true
				return
			}
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
}

type uploadMap struct {
	mutex   *sync.Mutex
	uploads map[string]*upload
}

func newUploadMap() uploadMap {
	return uploadMap{new(sync.Mutex), make(map[string]*upload)}
}

func (self *uploadMap) get(f Flow) *upload {
	self.mutex.Lock()
	upload, existed := self.uploads[f.Identifier]
	if !existed {
		upload = newUpload(f)
		self.uploads[f.Identifier] = upload
	}
	self.mutex.Unlock()
	return upload
}

func (self *uploadMap) remove(id string) {
	self.mutex.Lock()
	delete(self.uploads, id)
	self.mutex.Unlock()
}

type UploadHandler struct {
	uploads  uploadMap
	bs       *goblob.BlobService
	finished func(*http.Request, string)
}

func NewUploadHandler(bs *goblob.BlobService, uploadCompleted func(*http.Request, string)) *UploadHandler {
	u := newUploadMap()
	return &UploadHandler{u, bs, uploadCompleted}
}

func (self *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flow, chunkNumber, err := readFlow(r)
	if err != nil {
		http.Error(w, "Missing parameters in request", http.StatusBadRequest)
		return
	}
	upload := self.uploads.get(flow)

	if r.Method == "GET" {
		if _, found := upload.get(chunkNumber); found {
			return
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	} else if r.Method == "POST" {
		if _, found := upload.get(chunkNumber); found {
			return
		} else {
			defer func() {
				err := recover()
				if err != nil {
					http.Error(w, fmt.Sprintf("Something went wrong please try again: %s", err), http.StatusTeapot)
				}
			}()
			f, _, err := r.FormFile("file")
			if err != nil {
				log.Println("Error in finding the chunk data", err)
				http.Error(w, "not a form", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			chunkName := fmt.Sprintf("%v.chunk.%v", flow.Filename, chunkNumber)
			gf, err := self.bs.Create(chunkName)
			if err != nil {
				http.Error(w, "unable to open Mongo file", http.StatusTeapot)
				return
			}
			_, err = io.Copy(gf, f)
			if err != nil {
				http.Error(w, "unable to copy uploaded data to Mongo file", http.StatusTeapot)
				return
			}
			fileChunkId := gf.StringId()
			gf.Close()
			upload.put(chunkNumber, fileChunkId)

			upload.once.Do(func() {
				upload.writeOut = startWrite(upload, self.bs)
				go upload.writeOut.waitForParts()
			})

			upload.writeOut.tickle <- "We have more parts"

			if upload.hasAllChunks() {
				<-upload.writeOut.result
				self.uploads.remove(flow.Identifier)
				self.finished(r, upload.writeOut.outBlobId)
			}
		}
	}
}
