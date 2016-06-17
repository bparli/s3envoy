package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"s3envoy/hashes"
	"s3envoy/loadArgs"
	"s3envoy/queues"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/gorilla/mux"
)

var lru *queues.Queue
var mutex = &sync.Mutex{}

func s3Download(bucketName string, fkey string, fname string, dirName string) (file *os.File, numBytes int64) {
	file, _ = os.Create(fname)
	fullName := dirName + fkey
	//defer file.Close()
	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	numBytes, _ = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fullName),
		})
	return file, numBytes
}

func s3Upload(bucketName string, fkey string, localFname string, dirPath string, numBytes int64) {
	file, _ := os.Open(localFname)
	defer file.Close()

	buffer := make([]byte, numBytes)
	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	fullName := dirPath + fkey

	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName), // required
		Key:    aws.String(fullName),   // required
		Body:   fileBytes,
		Metadata: map[string]*string{
			"Key": aws.String(""), //required
		},
	}
	svc := s3.New(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	resp, err := svc.PutObject(params)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("file uploaded to s3, %s \n", resp)
}

func s3Get(w http.ResponseWriter, r *http.Request, fkey string, bucketName string, dirPath string, args *loadArgs.Args) {
	fname := args.LocalPath + fkey
	mutex.Lock()
	node, avail := lru.Retrieve(fkey)
	mutex.Unlock()

	if avail == false {
		fmt.Println("File not in local FS, download from S3")
		file, numBytes := s3Download(bucketName, fkey, fname, dirPath)
		if numBytes < args.MaxMemFileSize { //if small enough then add to memory
			d, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatal(err)
			}
			mutex.Lock()
			lru.Add(bucketName, dirPath, fkey, numBytes, true, d)
			mutex.Unlock()
		} else {
			mutex.Lock()
			lru.Add(bucketName, dirPath, fkey, numBytes, false, nil)
			mutex.Unlock()
		}
		http.ServeFile(w, r, fname)
	} else {
		fmt.Println("File in local FS")
		if node.Inmem == true {
			fmt.Println("File in local Mem", node.Fname)
			http.ServeContent(w, r, node.Fname, node.ModTime, node.MemFile)
		} else {
			//f := node.Path + node.Fname
			http.ServeFile(w, r, fname)
		}
	}
	fmt.Printf("Request for %s in %s of size \n", fkey, bucketName)
}

func s3GetHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) {
	vars := mux.Vars(r)
	fkey := vars["fkey"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	s3Get(w, r, fkey, bucketName, dirPath, args)
}

func uploader(bucketName string, fkey string, fname string, dirPath string, numBytes int64, id int, results chan<- int) {
	s3Upload(bucketName, fkey, fname, dirPath, numBytes)
	results <- 1
}

func s3Put(w http.ResponseWriter, r *http.Request, fkey string, bucketName string, dirPath string, args *loadArgs.Args) {
	localFname := args.LocalPath + fkey
	file, _ := os.Create(localFname)

	numBytes, err := io.Copy(file, r.Body)
	if err != nil {
		fmt.Fprintln(w, err)
	}
	file.Close()

	//new thread for background upload
	results := make(chan int, 1)
	go uploader(bucketName, fkey, localFname, dirPath, numBytes, 1, results)

	//add to local file queue
	if numBytes < args.MaxMemFileSize { //if small enough then add to memory too
		d, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		mutex.Lock()
		lru.Add(bucketName, fkey, dirPath, numBytes, true, d)
		mutex.Unlock()
	} else {
		mutex.Lock()
		lru.Add(bucketName, fkey, dirPath, numBytes, false, nil)
		mutex.Unlock()
	}
	//wait for s3 upload to finish
	<-results
	fmt.Fprintf(w, "File uploaded successfully : ")
}

func s3PutHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) {
	vars := mux.Vars(r)
	fkey := vars["fkey"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	s3Put(w, r, fkey, bucketName, dirPath, args)
}

func main() {
	args := loadArgs.Load()

	hashes.InitGH(args)
	lru = queues.InitializeQueue(args.TotalFiles, args.MemCap, args.DiskCap)
	router := mux.NewRouter() //.StrictSlash(true)
	// router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", s3GetHandler, args).Methods("GET")
	// router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", s3PutHandler, args).Methods("PUT")
	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3GetHandler(w, r, args)
	}).Methods("GET")

	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3PutHandler(w, r, args)
	}).Methods("PUT")

	http.ListenAndServe(":8080", router)
}
