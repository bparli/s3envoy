package hashes

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"s3envoy/loadArgs"
	"sync"
)

//Gh is the Global Hash struct
type Gh struct {
	Hash  map[string]map[string]int
	Mutex *sync.RWMutex
	args  *loadArgs.Args
}

//Ghash Global Hash table
var Ghash *Gh

//InitGH to initialize global hash from peers
func InitGH(args *loadArgs.Args) {
	newHash := make(map[string]map[string]int)
	Ghash = &Gh{Hash: newHash, Mutex: &sync.RWMutex{}, args: args}
}

//AddToGH adds a new record to the GH
func (h *Gh) AddToGH(fkey string, bucket string, peer string, send bool) {
	h.Mutex.Lock()
	p, ok := h.Hash[peer]
	if !ok {
		p = make(map[string]int)
		h.Hash[peer] = p
	}
	p[bucket+fkey] = 1
	h.Mutex.Unlock()

	go h.sendUpdates(fkey, bucket, true)
}

//RemoveFromGH updates the GH table with the eviction
func (h *Gh) RemoveFromGH(fkey string, bucket string, peer string, send bool) {
	h.Mutex.Lock()
	delete(h.Hash[peer], bucket+fkey)
	h.Mutex.Unlock()
	go h.sendUpdates(fkey, bucket, false)
}

//CheckGH to check if fkey is in any peer's store
func (h *Gh) CheckGH(fkey string, bucket string) string {
	h.Mutex.RLock()
	for peer, check := range h.Hash {
		_, ok := check[bucket+fkey]
		if ok == true {
			h.Mutex.RUnlock()
			return peer
		}
	}
	h.Mutex.RUnlock()
	return "None"
}

//SendUpdates will update all peers on a new entry to the local cache
func (h *Gh) sendUpdates(fkey string, bucket string, update bool) {
	upd := HashUpdate{peer: h.args.LocalName, bucketName: bucket, fkey: fkey, update: update}
	data, errM := json.Marshal(upd)
	if errM != nil {
		log.Fatal(errM)
	}
	buff := bytes.NewBuffer(data)

	for peer := range h.Hash {
		_, errReq := http.NewRequest("POST", peer, buff)
		if errReq != nil {
			log.Fatal(errReq)
		}
	}
}
