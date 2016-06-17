package hashes

import (
	"sync"
)

//Lh is the Global Hash struct
type Lh struct {
	Hash map[string]int
	Lock *sync.RWMutex
}

//Lhash Local Hash table
var Lhash *Lh

//InitLH to initialize global hash from peers
func InitLH() {
	newHash := make(map[string]int)
	Lhash = &Lh{Hash: newHash, Lock: &sync.RWMutex{}}
}

func (h *Lh) addToLH(fkey string, bucket string, peer string) {
	h.Lock.Lock()
	h.Hash[bucket+fkey] = 1
	h.Lock.Unlock()
}

func (h *Lh) removeFromLH(fkey string, bucket string) {
	h.Lock.Lock()
	delete(h.Hash, bucket+fkey)
	h.Lock.Unlock()
}

//CheckLH to check if fkey is in any peer's store
func (h *Lh) CheckLH(fkey string, bucket string) bool {
	h.Lock.RLock()
	_, ok := h.Hash[bucket+fkey]
	if ok == true {
		h.Lock.RUnlock()
		return true
	}
	h.Lock.RUnlock()
	return false
}
