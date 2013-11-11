package flow

import (
	"testing"
)

const cid = 42
const hex = "ABCD"

func TestPutChunk(t *testing.T) {
	upload := newUpload("/etc/gotube", 10)
	upload.put(cid, hex)
}

func TestGetChunk(t *testing.T) {
	upload := newUpload("settings.xml", 2)
	upload.put(cid, hex)
  if hex != upload.get(cid)  {
    t.Fail()
  }
}

func TestHasChunk(t *testing.T) {
	upload := newUpload("settings.xml", 2)
	upload.put(cid, hex)
  if ! upload.hasChunk(cid)  {
    t.Fail()
  }
}

func TestHasFinishedWriting(t *testing.T) {
}
