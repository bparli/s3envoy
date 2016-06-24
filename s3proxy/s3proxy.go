package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
var mutex = &sync.RWMutex{} //mutex to control access to shared lru struct

//AppError is the struct for error handling
type AppError struct {
	Error   error
	Message string
	Code    int
}

func s3Download(bucketName string, dirPath string, fname string, args *loadArgs.Args) (file *os.File, numBytes int64, err *AppError) {

	localPath := args.LocalPath + bucketName + "/" + dirPath
	errD := os.MkdirAll(localPath, 0755)
	if errD != nil {
		fmt.Println(errD, "Could not create local Directories")
		return nil, 0, &AppError{errD, "Could not create local Directories", 500}
	}
	file, errF := os.Create(localPath + fname)
	if errF != nil {
		fmt.Println(errD, "Could not create local File")
		return nil, 0, &AppError{errF, "Could not create local File", 500}
	}
	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	numBytes, errDown := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dirPath + fname),
		})
	if errDown != nil {
		fmt.Println(errD)
		return nil, 0, &AppError{errD, "Could not Dowload from S3", 500}
	}
	return file, numBytes, nil
}

func s3Upload(bucketName string, fkey string, localFname string, numBytes int64) *AppError {
	//read file from disk and upload to S3
	file, errF := os.Open(localFname)
	if errF != nil {
		return &AppError{errF, "Could not open local File", 500}
	}
	defer file.Close()

	buffer := make([]byte, numBytes)
	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)

	params := &s3.PutObjectInput{
		Bucket: aws.String(bucketName), // required
		Key:    aws.String(fkey),       // required
		Body:   fileBytes,
		Metadata: map[string]*string{
			"Key": aws.String(""), //required
		},
	}
	svc := s3.New(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	_, errU := svc.PutObject(params)
	if errU != nil {
		return &AppError{errU, "Could not Upload to S3", 500}
	}
	//fmt.Printf("file uploaded to s3, %s \n", resp)
	return nil
}

func s3Get(w http.ResponseWriter, r *http.Request, fname string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	//fmt.Println(args.LocalPath + bucketName + "/" + dirPath + fname)

	mutex.Lock()
	node, avail := lru.Retrieve(dirPath+fname, bucketName)
	mutex.Unlock()

	if avail == false {
		//fmt.Println("File not in local FS")

		//if in cluster mode, check if file is in Global Hash table
		res := "None"
		if args.Cluster == true {
			res = hashes.Ghash.CheckGH(dirPath+fname, bucketName)
			//fmt.Println(res)
		}
		if res == "None" {
			//fmt.Println("File not in local FS or Global Hash, download from S3")
			file, numBytes, errD := s3Download(bucketName, dirPath, fname, args)
			defer file.Close()
			if errD != nil {
				return errD
			}
			//if small enough then add to memory and disk.
			if numBytes < args.MaxMemFileSize {
				d, errR := ioutil.ReadAll(file)
				if errR != nil {
					return &AppError{errR, "Could read from file", 500}
				}
				mutex.Lock()
				lru.Add(bucketName, dirPath+fname, numBytes, true, d)
				mutex.Unlock()
			} else { //Otherwise just add to disk
				mutex.Lock()
				lru.Add(bucketName, dirPath+fname, numBytes, false, nil)
				mutex.Unlock()
			}
			http.ServeFile(w, r, args.LocalPath+bucketName+"/"+dirPath+fname)
		} else { //if in Global Hash then redirt to that host
			fmt.Print("File in Global Hash, Redirect client to Peer", res)
			http.Redirect(w, r, res, 307)
		}

	} else {
		//fmt.Println("File IS in local FS")
		if node.Inmem == true {
			//fmt.Println("File in local Mem", node.Fkey)
			mutex.RLock()
			http.ServeContent(w, r, node.Fkey, node.ModTime, node.MemFile)
			mutex.RUnlock()
		} else {
			http.ServeFile(w, r, args.LocalPath+bucketName+"/"+dirPath+fname)
		}
	}
	//fmt.Printf("Request for %s in %s \n", dirPath+fname, bucketName)
	return nil
}

func s3GetHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) *AppError {
	//get inputs from url and send to s3Get to download from S3.
	vars := mux.Vars(r)
	fname := vars["fname"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	err := s3Get(w, r, fname, bucketName, dirPath, args)
	//fmt.Println(args.LocalPath+bucketName+"/"+dirPath+fname, err)
	if err != nil {
		http.Error(w, err.Message, 500)
	}
	return nil
}

func uploader(bucketName string, fkey string, localFname string, numBytes int64, id int, results chan<- int) *AppError {
	//id int, results chan<- int
	err := s3Upload(bucketName, fkey, localFname, numBytes)
	results <- 1
	return err
}

func s3Put(w http.ResponseWriter, r *http.Request, fname string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	//key is the filename and full path.  Create a local file

	//fmt.Println(args.LocalPath + bucketName + "/" + dirPath + fname)

	localPath := args.LocalPath + bucketName + "/" + dirPath
	errD := os.MkdirAll(localPath, 0755)
	if errD != nil {
		return &AppError{errD, "Could not create local Directories", 500}
	}
	file, errF := os.Create(localPath + fname)
	if errF != nil {
		return &AppError{errF, "Could not create local File", 500}
	}

	numBytes, errC := io.Copy(file, r.Body)
	if errC != nil {
		return &AppError{errC, "Could not Copy to local File", 500}
	}
	file.Close()

	fmt.Println("Test Add to GH", args.Cluster, dirPath+fname, bucketName, args.LocalName)
	//new thread for background S3 upload
	results := make(chan int, 1)
	go uploader(bucketName, dirPath+fname, localPath+fname, numBytes, 1, results)
	//go uploader(bucketName, dirPath+fname, localPath+fname, numBytes)

	fmt.Println(args.Cluster)
	if args.Cluster == true {
		fmt.Println("Add to GH", dirPath+fname, bucketName, args.LocalName)
		go hashes.Ghash.AddToGH(dirPath+fname, bucketName, args.LocalName, true)
	}

	//add to local file queue
	if numBytes < args.MaxMemFileSize { //if small enough then add to memory too
		d, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return &AppError{errC, "Could not Read from local File", 500}
		}
		mutex.Lock()
		lru.Add(bucketName, dirPath+fname, numBytes, true, d)
		mutex.Unlock()
	} else {
		mutex.Lock()
		lru.Add(bucketName, dirPath+fname, numBytes, false, nil)
		mutex.Unlock()
	}
	//wait for s3 upload to finish
	<-results
	fmt.Fprintf(w, "File uploaded successfully : ")

	return nil
}

func s3PutHandler(w http.ResponseWriter, r *http.Request, args *loadArgs.Args) *AppError {
	//get inputs from url and send to s3Put to upload to S3.  All PUT requests get written
	//to S3 and local even if they already exists
	vars := mux.Vars(r)
	fname := vars["fname"]
	bucket := vars["bucket"]
	splits := strings.SplitN(bucket, "/", 2)
	bucketName := splits[0]
	dirPath := splits[1]
	err := s3Put(w, r, fname, bucketName, dirPath, args)
	if err != nil {
		fmt.Println("Error in PUT", args.LocalPath+bucketName+"/"+dirPath+fname, err)
		http.Error(w, err.Message, 500)
	}
	return nil
}

func main() {
	var conf = flag.String("config", "/Users/bparli/go/bin/config.json", "location of config.json")
	var port = flag.String("port", "8080", "server port number")
	flag.Parse()
	//load arguments from config.json
	args := loadArgs.Load(*conf)
	//args.LocalName = args.LocalName + *port

	//initialize the local LRU queue
	lru = queues.InitializeQueue(args)

	//based on arguments, if clustered then initialize the global hash table
	if args.Cluster == true {
		hashes.InitGH(args)
		go hashes.HashMan(args.HashPort)
	}

	//use mux router and handler functions with the args struct being passed in
	router := mux.NewRouter() //.StrictSlash(true)
	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fname:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3GetHandler(w, r, args)
	}).Methods("GET")

	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fname:[a-zA-Z0-9-\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3PutHandler(w, r, args)
	}).Methods("PUT")

	http.ListenAndServe(":"+*port, router)
}
