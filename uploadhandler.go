package flow

import (
	"fmt"
	"github.com/slspeek/goblob"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type flow struct {
	identifier  string
	filename    string
	totalChunks int
	chunkSize   int
}

func readFlow(r *http.Request) (f flow, chunkNumber int, err error) {
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
	f = flow{identifier, filename, totalChunks, chunkSize}
	return
}

type upload struct {
	mutex      *sync.Mutex
	chunks     map[int]string
	filename   string
	chunkCount int
	once       *sync.Once
	finished   chan bool
}

func newUpload(fn string, chunkCount int) upload {
	return upload{new(sync.Mutex), make(map[int]string), fn, chunkCount, new(sync.Once), make(chan bool)}
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

func (self *upload) concatFile(bs *goblob.BlobService) (string, error) {
	file, err := bs.Create(self.filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	for i := 1; i <= self.chunkCount; i++ {
		chunk := self.getChunk(i, bs)
		_, err = io.Copy(file, chunk)
		if err != nil {
			return "", err
		}
		chunk.Close()
		bs.Remove(chunk.Id())
	}
	return file.Id(), nil
}

func (self *upload) hasAllChunks() bool {
	for i := 1; i <= self.chunkCount; i++ {
		if _, found := self.get(i); !found {
			return false
		}
	}
	return true
}

type uploadMap struct {
	mutex   *sync.Mutex
	uploads map[string]upload
}

func newUploadMap() uploadMap {
	return uploadMap{new(sync.Mutex), make(map[string]upload)}
}

func (self *uploadMap) get(id string, fn string, chc int) upload {
	self.mutex.Lock()
	upload, existed := self.uploads[id]
	if !existed {
		upload = newUpload(fn, chc)
		self.uploads[id] = upload
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
	upload := self.uploads.get(flow.identifier, flow.filename, flow.totalChunks)

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
					http.Error(w, "Something went wrong please try again", http.StatusTeapot)
				}
			}()
			f, _, err := r.FormFile("file")
			if err != nil {
				log.Println("Error in finding the chunk data", err)
				http.Error(w, "not a form", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			chunkName := fmt.Sprintf("%v.chunk.%v", flow.filename, chunkNumber)
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
			fileChunkId := gf.Id()
			gf.Close()
			upload.put(chunkNumber, fileChunkId)
			if upload.hasAllChunks() {
				upload.once.Do(func() {
					go func() {
						log.Println("Starting concat for ", flow.identifier)
						fileId, err := upload.concatFile(self.bs)
						if err != nil {
							log.Println("Error during concat of: ", flow.identifier, " ", err)
							return
						}
						self.uploads.remove(flow.identifier)
						self.finished(r, fileId)
						upload.finished <- true
					}()
				})
			}
		}
	}
}
