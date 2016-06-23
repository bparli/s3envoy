package queues

import (
	"fmt"
	"os"
	"s3envoy/hashes"
	"s3envoy/loadArgs"
	"time"
)

//Node struct for each local file node
type Node struct {
	dirty      bool //  dirty or clean
	Bucket     string
	LocalFname string
	Fkey       string
	size       int64
	Inmem      bool     //is file small enough to be in memory
	MemFile    *MemFile //only if file is in memory
	ModTime    time.Time
	prev       *Node
	next       *Node
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
	args       *loadArgs.Args //program arguments
	Gh         *hashes.Gh
}

//InitializeQueue global LRU
func InitializeQueue(args *loadArgs.Args) *Queue {
	new := &Queue{totalFiles: args.TotalFiles, currFiles: 0, diskCap: args.DiskCap, currDisk: 0,
		memCap: args.MemCap, currMem: 0, head: nil, tail: nil,
		args: args}
	return new
}

func (lru *Queue) getTail() *Node {
	return lru.tail
}

func (lru *Queue) getHead() *Node {
	return lru.head
}

//Retrieve page from global LRU
func (lru *Queue) Retrieve(fkey string, bucket string) (*Node, bool) {
	if lru.currFiles == 0 {
		return nil, false
	}
	tmp := lru.getHead()
	for i := 0; i < lru.currFiles-1; i++ {
		if tmp.Fkey == fkey && tmp.Bucket == bucket {
			lru.moveToHead(tmp)
			return tmp, true
		}
		tmp = tmp.next
	}
	return nil, false
}

func (lru *Queue) evict() {
	//evict current tail and adjust len -1 node to be new tail
	currT := lru.getTail()
	os.Remove(lru.args.LocalPath + currT.Fkey)
	newT := currT.prev
	newT.next = nil
	lru.tail = newT
	lru.currFiles--
	lru.currMem += currT.size
	lru.currDisk += currT.size

	if lru.args.Cluster == true {
		go hashes.Ghash.RemoveFromGH(currT.Fkey, currT.Bucket, lru.args.LocalName, true)
	}

	return
}

func (lru *Queue) moveToHead(move *Node) {
	//add new node to head of queue and shift current head down 1
	tmp := lru.getHead()
	if tmp.Fkey == move.Fkey {
		return
	}
	fmt.Println("move to head", move.Fkey)

	//if node in queue find it and move it to head, then clean up linked list
	if lru.currFiles > 1 {
		for i := 0; i < lru.currFiles-1; i++ {
			if tmp.Fkey == move.Fkey && tmp.Bucket == move.Bucket {
				prev := tmp.prev
				if tmp.next != nil {
					next := tmp.next
					next.prev = prev
					prev.next = next
				} else {
					prev.next = nil
					lru.tail = prev
				}
				break
			}
			tmp = tmp.next
		}
		oldH := lru.getHead()
		move.next = oldH
		move.prev = nil
		oldH.prev = move
		lru.head = move
	}
	return
}

//Add missing file to LRU.  new file goes to head of queue
func (lru *Queue) Add(bucket string, fkey string, size int64, inmem bool, data []byte) (*Node, error) {
	//add node to LRU queue and evict if already full
	_, queued := lru.Retrieve(fkey, bucket)
	if queued == true {
		return nil, nil
	}
	new := &Node{dirty: false, Bucket: bucket, Fkey: fkey,
		LocalFname: lru.args.LocalPath + bucket + fkey,
		size:       size, ModTime: time.Now(), prev: nil, next: nil}
	if inmem == true {
		new.Inmem = true
		newMem := &MemFile{offset: 0, dirOffset: 0, Content: data}
		new.MemFile = newMem
	} else {
		new.Inmem = false
	}

	//pop objects off the end of the queue if we need room
	for {
		if (lru.currMem+size) > lru.memCap || (lru.currDisk+size) > lru.diskCap {
			lru.evict()
		} else {
			break
		}

	}
	if lru.args.Cluster == true {
		go hashes.Ghash.AddToGH(fkey, bucket, lru.args.LocalName, true)
	}

	if lru.currFiles == 0 {
		lru.head = new
		lru.tail = new
		lru.currFiles++
		lru.currMem += size
		lru.currDisk += size
		return new, nil
	}

	lru.moveToHead(new)
	lru.currFiles++
	lru.currMem += size
	lru.currDisk += size
	return new, nil
}
