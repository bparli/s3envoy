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

//AppError is the struct for error handling
type AppError struct {
	Error   error
	Message string
	Code    int
}

func s3Download(bucketName string, fkey string, fname string, dirName string) (file *os.File, numBytes int64, err *AppError) {
	file, errF := os.Create(fname)
	if errF != nil {
		return nil, 0, &AppError{errF, "Could not create local File", 500}
	}
	fullName := dirName + fkey
	//defer file.Close()
	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	numBytes, errD := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(fullName),
		})
	if errD != nil {
		return nil, 0, &AppError{errD, "Could not Dowload from S3", 500}
	}
	return file, numBytes, nil
}

func s3Upload(bucketName string, fkey string, localFname string, dirPath string, numBytes int64) *AppError {
	file, errF := os.Open(localFname)
	if errF != nil {
		return &AppError{errF, "Could not create local File", 500}
	}
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
	resp, errU := svc.PutObject(params)
	if errU != nil {
		return &AppError{errU, "Could not Upload to S3", 500}
	}
	fmt.Printf("file uploaded to s3, %s \n", resp)
	return nil
}

func s3Get(w http.ResponseWriter, r *http.Request, fkey string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	fname := args.LocalPath + fkey
	mutex.Lock()
	node, avail := lru.Retrieve(fkey)
	mutex.Unlock()

	if avail == false {
		fmt.Println("File not in local FS, download from S3")
		res := hashes.Ghash.CheckGH(fkey, bucketName)
		if res == "None" {
			file, numBytes, errD := s3Download(bucketName, fkey, fname, dirPath)
			if errD != nil {
				return errD
			}
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
			http.Redirect(w, r, res, 307)
		}

	} else {
		fmt.Println("File in local FS")
		if node.Inmem == true {
			fmt.Println("File in local Mem", node.Fname)
			http.ServeContent(w, r, node.Fname, node.ModTime, node.MemFile)
		} else {
			http.ServeFile(w, r, fname)
		}
	}
	fmt.Printf("Request for %s in %s of size \n", fkey, bucketName)
	return nil
}

func s3GetHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) *AppError {
	vars := mux.Vars(r)
	fkey := vars["fkey"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	err := s3Get(w, r, fkey, bucketName, dirPath, args)
	if err != nil {
		http.Error(w, err.Message, 500)
	}
	return nil
}

func uploader(bucketName string, fkey string, fname string, dirPath string, numBytes int64, id int, results chan<- int) *AppError {
	err := s3Upload(bucketName, fkey, fname, dirPath, numBytes)
	results <- 1
	return err
}

func s3Put(w http.ResponseWriter, r *http.Request, fkey string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	localFname := args.LocalPath + fkey
	file, errF := os.Create(localFname)
	if errF != nil {
		return &AppError{errF, "Could not create local File", 500}
	}

	numBytes, errC := io.Copy(file, r.Body)
	if errC != nil {
		return &AppError{errC, "Could not Copy to local File", 500}
	}
	file.Close()

	//new thread for background upload
	results := make(chan int, 1)
	go uploader(bucketName, fkey, localFname, dirPath, numBytes, 1, results)
	go hashes.Ghash.AddToGH(fkey, bucketName, args.LocalName, true)

	//add to local file queue
	if numBytes < args.MaxMemFileSize { //if small enough then add to memory too
		d, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return &AppError{errC, "Could not Read from local File", 500}
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

	return nil
}

func s3PutHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) *AppError {
	vars := mux.Vars(r)
	fkey := vars["fkey"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	err := s3Put(w, r, fkey, bucketName, dirPath, args)
	if err != nil {
		http.Error(w, err.Message, 500)
	}
	return nil
}

func main() {
	args := loadArgs.Load()

	if args.Cluster == true {
		hashes.InitGH(args)
	}

	lru = queues.InitializeQueue(args)
	router := mux.NewRouter() //.StrictSlash(true)
	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3GetHandler(w, r, args)
	}).Methods("GET")

	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fkey:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3PutHandler(w, r, args)
	}).Methods("PUT")

	http.ListenAndServe(":8080", router)
}
