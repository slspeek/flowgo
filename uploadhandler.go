package flow

import (
  "sync"
)

type Upload struct {
  mutex sync.Mutex
  chunks map[int]string
}


