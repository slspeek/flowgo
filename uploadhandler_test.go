package flow

import (
	"testing"
)

const cid = 42
const hex = "ABCD"

func TestPutChunk(t *testing.T) {
	upload := newUpload()
	upload.Put(cid, hex)
}

func TestGetChunk(t *testing.T) {

	upload := newUpload()
	upload.Put(cid, hex)
  if hex != upload.Get(cid)  {
    t.Fail()
  }
}

func TestGetUpload(t *testing.T) {

