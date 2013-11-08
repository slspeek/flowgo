package flow

import (
  "github.com/slspeek/goblob"
  "sync"
)

type upload struct {
  mutex sync.Mutex
  chunks map[int]string
}

type uploadMap struct {
  mutex sync.Mutex
  uploads map[string]upload
}

type UploadHandler struct {
  uploads uploadMap
  bs      goblob.BlobService
}
