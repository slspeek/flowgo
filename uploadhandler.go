package flow

import (
	"github.com/slspeek/goblob"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
)

type upload struct {
	mutex         *sync.Mutex
	writeOutMutex *sync.Mutex
	chunks        map[int]string
	blobId        string
	bs            *goblob.BlobService
	filename      string
	chunkCount    int
	lastWrote     int
}

func newUpload(fn string, chunkCount int, bs *goblob.BlobService) upload {
	return upload{new(sync.Mutex), new(sync.Mutex), make(map[int]string), "", bs, fn, chunkCount, 1}
}

func (self *upload) put(chunkId int, blobId string) {
	self.mutex.Lock()
	self.chunks[chunkId] = blobId
	self.mutex.Unlock()
}

func (self *upload) hasChunk(chunkId int) bool {
	self.mutex.Lock()
	_, result := self.chunks[chunkId]
	self.mutex.Unlock()
	return result
}

func (self *upload) get(chunkId int) string {
	self.mutex.Lock()
	result, _ := self.chunks[chunkId]
	self.mutex.Unlock()
	return result
}

func (self *upload) getChunk(chunkId int) *goblob.File {
	self.mutex.Lock()
	id, _ := self.chunks[chunkId]
	self.mutex.Unlock()
	chunk, _ := self.bs.Open(id)
	return chunk
}

func (self *upload) finishedWriting() (string, bool) {
	self.writeOutMutex.Lock()
	defer self.writeOutMutex.Unlock()
	if self.hasAllChunks() {
		file, err := self.bs.Create(self.filename)
		if err != nil {
			return "", false
		}
		defer file.Close()
		//file.Seek(0, 2)
		log.Println("Blob opened yy")
		for i := 1; i <= self.chunkCount; i++ {
			log.Println("Iterating", i)
			if self.hasChunk(i) {
				chunk := self.getChunk(i)

				_, err = io.Copy(file, chunk)
        if err != nil {
          panic("Copy to total flow file failed");
        }
				chunk.Close()
			} else {
				return "", false
			}
		}
		return file.Id(), true
	} else {
		return "", false
	}
}

func (self *upload) hasAllChunks() bool {
	for i := 1; i <= self.chunkCount; i++ {
		if !self.hasChunk(i) {
			return false
		}
	}
	return true
}

type uploadMap struct {
	mutex   *sync.Mutex
	uploads map[string]upload
	bs      *goblob.BlobService
}

func newUploadMap(bs *goblob.BlobService) uploadMap {
	return uploadMap{new(sync.Mutex), make(map[string]upload), bs}
}

func (self *uploadMap) get(id string, fn string, chc int) upload {
	self.mutex.Lock()
	upload, existed := self.uploads[id]
	if !existed {
		upload = newUpload(fn, chc, self.bs)
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
	finished func(http.ResponseWriter, *http.Request, string)
}

func NewUploadHandler(bs *goblob.BlobService, uploadCompleted func(http.ResponseWriter, *http.Request, string)) *UploadHandler {
	u := newUploadMap(bs)
	return &UploadHandler{u, bs, uploadCompleted}
}

func (self *UploadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flowFileIdentifier := r.FormValue("flowFileIdentifier")
	flowFilename := r.FormValue("flowFilename")
	flowChunkNumber, err := strconv.Atoi(r.FormValue("flowChunkNumber"))
	if err != nil {
		http.Error(w, "Invalid flowChunkNumber", http.StatusBadRequest)
	}
	flowChunkCount, err := strconv.Atoi(r.FormValue("flowTotalChunks"))
	if err != nil {
		http.Error(w, "Invalid flowTotalChunks", http.StatusBadRequest)
	}

	upload := self.uploads.get(flowFileIdentifier, flowFilename, flowChunkCount)
	if r.Method == "GET" {
		if upload.hasChunk(flowChunkNumber) {
			return
		} else {
			http.Error(w, "not found", http.StatusNotFound)
		}
	} else if r.Method == "POST" {
		if upload.hasChunk(flowChunkNumber) {
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
				log.Println(err)
				http.Error(w, "not a form", http.StatusBadRequest)
			}
			err = r.ParseForm()
			if err != nil {
				log.Println(err)
				http.Error(w, "not a form", http.StatusBadRequest)
			}

			defer r.Body.Close()

			chunkName := flowFilename + ".chunk." + string(flowChunkNumber)
			gf, err := self.bs.Create(chunkName)
			if err != nil {
				http.Error(w, "unable to open Mongo file", http.StatusTeapot)
			}
			_, err = io.Copy(gf, f)
			if err != nil {
				http.Error(w, "unable to copy uploaded data to Mongo file", http.StatusTeapot)

			}
			fileChunkId := gf.Id()
			gf.Close()
			upload.put(flowChunkNumber, fileChunkId)
      go func() {
        blobId, finished := upload.finishedWriting()
        if finished {
          self.finished(w, r, blobId)
        }
      }()
		}
	}
}
