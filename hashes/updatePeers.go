package hashes

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

//UpdatePeers will send an json update to each peer in the cluster
func UpdatePeers(peers []string, fkey string, bucketName string, localName string) {
	for peer := range peers {
		upd := HashUpdate{peer: localName, bucketName: bucketName, fkey: fkey, update: true}
		data, errM := json.Marshal(upd)
		if errM != nil {
			log.Fatal(errM)
		}

		buff := bytes.NewBuffer(data)
		_, errR := http.NewRequest("POST", "http://"+peers[peer]+":9080", buff)
		if errR != nil {
			log.Fatal(errR)
		}
	}
}
