package hashes

import (
	"bytes"
	"encoding/json"
	"net/http"
	"s3envoy/loadArgs"
	"sync"

	log "github.com/Sirupsen/logrus"
)

//Gh is the Global Hash struct
type Gh struct {
	Hash  map[string]string
	Mutex *sync.RWMutex
	args  *loadArgs.Args
}

//Ghash Global Hash table
var Ghash *Gh

//InitGH to initialize global hash from peers
func InitGH(args *loadArgs.Args) {
	newHash := make(map[string]string) //hash is a map of file keys (bucket+fkey), mapped to a peer
	Ghash = &Gh{Hash: newHash, Mutex: &sync.RWMutex{}, args: args}
}

//AddToGH adds a new record to the GH
func (h *Gh) AddToGH(fkey string, bucket string, peer string, send bool) {
	log.Debugln("Add to global hash", bucket, fkey, send)
	h.Mutex.Lock()
	h.Hash[bucket+"/"+fkey] = peer //update the peer to contain the bucket+fkey value
	h.Mutex.Unlock()
	if send == true {
		h.sendUpdates(fkey, bucket, "true")
	}
}

//RemoveFromGH updates the GH table with the eviction
func (h *Gh) RemoveFromGH(fkey string, bucket string, send bool) {
	h.Mutex.Lock()
	delete(h.Hash, bucket+"/"+fkey)
	h.Mutex.Unlock()
	if send == true {
		go h.sendUpdates(fkey, bucket, "false")
	}
}

//CheckGH to check if fkey is in any peer's store
func (h *Gh) CheckGH(fkey string, bucket string) string {
	h.Mutex.RLock()
	peer, ok := h.Hash[bucket+"/"+fkey]
	if !ok {
		h.Mutex.RUnlock()
		return "None"
	}
	h.Mutex.RUnlock()
	return peer
}

//SendUpdates will update all peers on a new entry to the local cache.  update reflects whether
//something should be in the hash table (true) or not (false)
func (h *Gh) sendUpdates(fkey string, bucket string, update string) {
	for _, peer := range h.args.Peers {
		if h.args.CheckMemberAlive(peer) == true {
			log.Debugln("Send update to peer:", peer)
			upd := &HashUpdate{Peer: h.args.LocalName, BucketName: bucket, Fkey: fkey, Update: update}
			data, errM := json.Marshal(upd)
			if errM != nil {
				log.Fatal(errM)
			}
			buff := bytes.NewBuffer(data)
			log.Debugln(peer)
			req, err := http.NewRequest("POST", "http://"+peer, buff)
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}
			defer resp.Body.Close()
		}
	}
}
