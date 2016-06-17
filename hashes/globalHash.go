package hashes

import (
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

func (h *Gh) addToGH(fkey string, bucket string, peer string) {
	h.Mutex.Lock()
	p, ok := h.Hash[peer]
	if !ok {
		p = make(map[string]int)
		h.Hash[peer] = p
	}
	p[bucket+fkey] = 1
	h.Mutex.Unlock()
}

func (h *Gh) removeFromGH(fkey string, bucket string, peer string) {
	h.Mutex.Lock()
	delete(h.Hash[peer], bucket+fkey)
	h.Mutex.Unlock()
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
