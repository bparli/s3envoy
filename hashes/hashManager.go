package hashes

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

//HashUpdate to send and receive updates to the global hash table
type HashUpdate struct {
	peer       string
	bucketName string
	fkey       string
	update     bool //true = add and false = remove
}

func globalHashMan(w http.ResponseWriter, r *http.Request) {
	update := new(HashUpdate)
	json.NewDecoder(r.Body).Decode(update)
	Ghash.Mutex.Lock()
	if update.update == true {
		Ghash.AddToGH(update.fkey, update.bucketName, update.peer, false)
	} else {
		Ghash.RemoveFromGH(update.fkey, update.bucketName, update.peer, false)
	}
	Ghash.Mutex.Unlock()
}

//HashMan listens for updates from peers
func HashMan() {
	router := mux.NewRouter() //.StrictSlash(true)
	router.HandleFunc("/", globalHashMan)
	http.ListenAndServe(":9080", router)
}
