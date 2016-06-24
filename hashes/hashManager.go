package hashes

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

//HashUpdate to send and receive updates to the global hash table
type HashUpdate struct {
	Peer       string
	BucketName string
	Fkey       string
	Update     string //true = add and false = remove
}

func globalHashMan(w http.ResponseWriter, r *http.Request) {
	update := new(HashUpdate)
	err := json.NewDecoder(r.Body).Decode(update)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Update: ", update.Peer, update.BucketName, update.Fkey)

	if update.Update == "true" {
		Ghash.AddToGH(update.Fkey, update.BucketName, update.Peer, false)
	} else {
		Ghash.RemoveFromGH(update.Fkey, update.BucketName, false)
	}
	//fmt.Fprintf(w, "Updated")
}

//HashMan listens for updates from peers
func HashMan(port string) {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", globalHashMan)
	http.ListenAndServe(":"+port, router)
}
