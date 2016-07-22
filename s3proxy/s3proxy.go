package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"s3envoy/hashes"
	"s3envoy/loadArgs"
	"s3envoy/queues"
	"strings"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/Nitro/memberlist"
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

func CheckFileInPeerNode(fkey string, bucketName string, args *loadArgs.Args) (bool, string) {
	res := "None"
	res = hashes.Ghash.CheckGH(fkey, bucketName)
	if res == "None" {
		return false, ""
	}
	if args.CheckMemberAlive(res) {
		return true, res
	}
	return false, ""
}

func s3Download(bucketName string, dirPath string, fname string, args *loadArgs.Args) (file *os.File, numBytes int64, Apperr *AppError) {

	localPath := args.LocalPath + bucketName + "/" + dirPath
	err := os.MkdirAll(localPath, 0755)
	if err != nil {
		log.Errorln(err, "Could not create local Directories")
		return nil, 0, &AppError{err, "Could not create local Directories", 500}
	}
	file, err = os.Create(localPath + fname)
	if err != nil {
		log.Errorln(err, "Could not create local File")
		return nil, 0, &AppError{err, "Could not create local File", 500}
	}
	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String("us-west-1")}))
	numBytes, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(dirPath + fname),
		})
	if err != nil {
		log.Errorln(err)
		return nil, 0, &AppError{err, "Could not Dowload from S3", 500}
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
	log.Infoln("file uploaded to s3")
	return nil
}

func s3Get(w http.ResponseWriter, r *http.Request, fname string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	mutex.Lock()
	node, avail := lru.Retrieve(dirPath+fname, bucketName)
	mutex.Unlock()

	if avail == false {
		log.Debugln("File not in local FS")

		//if in cluster mode, check if file is in Global Hash table
		var check bool
		var res string
		if args.Cluster == true {
			check, res = CheckFileInPeerNode(dirPath+fname, bucketName, args)
		}

		if check == false {
			log.Debugln("File not in local FS or Global Hash, download from S3")
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
			log.Debugln("File in Global Hash, Redirect client to Peer", res)
			http.Redirect(w, r, res+"/"+bucketName+"/"+dirPath+fname, 307)
		}

	} else {
		log.Debugln("File IS in local FS")
		if node.Inmem == true {
			mutex.RLock()
			http.ServeContent(w, r, node.Fkey, node.ModTime, node.MemFile)
			mutex.RUnlock()
		} else {
			http.ServeFile(w, r, args.LocalPath+bucketName+"/"+dirPath+fname)
		}
	}
	log.Debugln("Request for ", dirPath+fname, bucketName)
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
	if err != nil {
		http.Error(w, err.Message, 500)
	}
	return nil
}

func uploader(bucketName string, fkey string, localFname string, numBytes int64) *AppError {
	//id int, results chan<- int
	err := s3Upload(bucketName, fkey, localFname, numBytes)
	if err != nil {
		log.Errorln("S3 upload Error:", err)
	}
	//results <- 1
	return err
}

func s3Put(w http.ResponseWriter, r *http.Request, fname string, bucketName string, dirPath string, args *loadArgs.Args) *AppError {
	//key is the filename and full path.  Create a local file

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

	//new thread for background S3 upload
	//results := make(chan int, 1)
	//go uploader(bucketName, dirPath+fname, localPath+fname, numBytes, 1, results)
	go uploader(bucketName, dirPath+fname, localPath+fname, numBytes)

	//log.Debugln(args.Cluster)
	if args.Cluster == true {
		log.Debugln("Add to GH", dirPath+fname, bucketName, args.LocalName)
		go hashes.Ghash.AddToGH(dirPath+fname, bucketName, args.LocalName, true)
	}

	//add to local file queue
	if numBytes < args.MaxMemFileSize { //if small enough then add to memory too
		d, err := ioutil.ReadAll(r.Body)
		defer r.Body.Close()
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
	//<-results
	log.Infoln("File uploaded successfully")
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
		log.Errorln("Error in PUT", args.LocalPath+bucketName+"/"+dirPath+fname, err)
		http.Error(w, err.Message, 500)
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "File Uploading")
	return nil
}

func main() {
	runtime.GOMAXPROCS(2)

	log.SetLevel(log.DebugLevel)

	var conf = flag.String("config", "/Users/bparli/go/bin/config.json", "location of config.json")
	var port = flag.String("port", "8080", "server port number")
	flag.Parse()

	//load arguments from config.json
	args := loadArgs.Load(*conf)

	//initialize the local LRU queue
	lru = queues.InitializeQueue(args)

	//based on arguments, if clustered then initialize the global hash table
	if args.Cluster == true {
		hashes.InitGH(args)
		go hashes.HashMan(args.HashPort)
	}

	var err error
	memberlistConfig := memberlist.DefaultLocalConfig()
	localIP := strings.Split(args.LocalName, ":")[0]
	memberlistConfig.AdvertiseAddr = localIP

	args.Members, err = memberlist.Create(memberlistConfig)
	if err != nil {
		log.Errorln("Failed to create memberlist: " + err.Error())
	}

	// Join an existing cluster by specifying at least one known member.
	var memberIPs []string
	for _, peer := range args.Peers {
		peerIP := strings.Split(peer, ":")[0]
		memberIPs = append(memberIPs, peerIP)
	}
	_, err = args.Members.Join(memberIPs)
	if err != nil {
		log.Errorln("Failed to join cluster: " + err.Error())
	}

	//use mux router and handler functions with the args struct being passed in
	router := mux.NewRouter() //.StrictSlash(true)
	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*\\/}{fname:[a-zA-Z0-9-_\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3PutHandler(w, r, args)
	}).Methods("PUT", "POST")
	router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*[\\/]+}{fname:[a-zA-Z0-9-_\\.]*$}", func(w http.ResponseWriter, r *http.Request) {
		//router.HandleFunc("/{bucket:[a-zA-Z0-9-\\.\\/]*[\\/]+}{fname:[.]*$}", func(w http.ResponseWriter, r *http.Request) {
		s3GetHandler(w, r, args)
	}).Methods("GET")

	http.ListenAndServe(":"+*port, router)
}
