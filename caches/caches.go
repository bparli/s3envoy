package caches

import (
	"fmt"
	"os"
	"time"
)

//Node struct for each local file node
type Node struct {
	dirty   bool //  dirty or clean
	Bucket  string
	dirPath string
	Fname   string
	size    int64
	Inmem   bool     //is file small enough to be in memory
	MemFile *MemFile //only if file is in memory
	ModTime time.Time
	prev    *Node
	next    *Node
}

//Queue struct for local files
type Queue struct {
	totalFiles int   // number of files allowed to be held locally
	currFiles  int   //number of current files help locally
	diskCap    int64 //total storage size in bytes
	currDisk   int64 //current storage size in bytes
	memCap     int64 //total storage size in bytes
	currMem    int64 //current storage size in bytes
	head       *Node
	tail       *Node
}

//InitializeQueue global LRU
func InitializeQueue(cap int, maxMem int64, maxDisk int64) *Queue {
	new := &Queue{totalFiles: cap, currFiles: 0, diskCap: maxDisk, currDisk: 0,
		memCap: maxMem, currMem: 0, head: nil, tail: nil}
	return new
}

func (hash *Queue) getTail() *Node {
	return hash.tail
}

func (hash *Queue) getHead() *Node {
	return hash.head
}

//Retrieve page from global LRU
func (hash *Queue) Retrieve(fname string) (*Node, bool) {
	if hash.currFiles == 0 {
		return nil, false
	}
	tmp := hash.getHead()
	for i := 0; i < hash.currFiles-1; i++ {
		if tmp.Fname == fname {
			hash.moveToHead(tmp)
			return tmp, true
		}
		tmp = tmp.next
	}
	return nil, false
}

func (hash *Queue) evict() {
	//evict current tail and adjust len -1 node to be new tail
	currT := hash.getTail()
	os.Remove(currT.Fname)
	newT := currT.prev
	newT.next = nil
	hash.tail = newT
	hash.currFiles--
	hash.currMem += currT.size
	hash.currDisk += currT.size

	return
}

func (hash *Queue) moveToHead(move *Node) {
	//add new node to head and shift current head down 1
	tmp := hash.getHead()
	if tmp.Fname == move.Fname {
		return
	}
	if hash.currFiles > 1 {
		for i := 0; i < hash.currFiles-1; i++ {
			fmt.Println("move to head", hash.currFiles, move.Fname)
			if tmp.Fname == move.Fname {
				prev := tmp.prev
				if tmp.next != nil {
					next := tmp.next
					next.prev = prev
					prev.next = next
				} else {
					prev.next = nil
					hash.tail = prev
				}
				break
			}
			tmp = tmp.next
		}
		oldH := hash.getHead()
		move.next = oldH
		move.prev = nil
		oldH.prev = move
		hash.head = move
	}
	return
}

//Add missing file to global LRU
func (hash *Queue) Add(bucket string, dirPath string, fname string, size int64, inmem bool, data []byte) (*Node, error) {
	//add node to LRU queue and evict if already full
	_, queued := hash.Retrieve(fname)
	//println(hash.currDisk, hash.currMem)
	//fmt.Println(queued, hash.currFiles, hash.totalFiles)

	if queued == true {
		return nil, nil
	}
	new := &Node{dirty: false, Bucket: bucket,
		dirPath: dirPath, Fname: fname,
		size: size, ModTime: time.Now(), prev: nil, next: nil}
	if inmem == true {
		new.Inmem = true
		newMem := &MemFile{offset: 0, dirOffset: 0, Content: data}
		new.MemFile = newMem
	} else {
		new.Inmem = false
	}

	for {
		if (hash.currMem+size) > hash.memCap || (hash.currDisk+size) > hash.diskCap {
			hash.evict()
		} else {
			break
		}
	}
	if hash.currFiles == 0 {
		hash.head = new
		hash.tail = new
		hash.currFiles++
		hash.currMem += size
		hash.currDisk += size
		return new, nil
	}

	hash.moveToHead(new)
	hash.currFiles++
	hash.currMem += size
	hash.currDisk += size
	return new, nil
}
