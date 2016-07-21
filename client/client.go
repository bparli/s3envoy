package main

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func s3Upload(bucketName string, fkey string, localFname string) error {
	//read file from disk and upload to S3
	file, errO := os.Open(localFname)
	if errO != nil {
		log.Errorln("Error opening local file", errO)
	}
	defer file.Close()

	numBytes, errS := file.Stat()
	if errS != nil {
		log.Errorln("Error getting Stat of local file", errS)
	}
	buffer := make([]byte, numBytes.Size())
	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)

	log.Debugln("load File to S3")
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
		log.Errorln("Error uploading file", errU)
	}
	log.Infoln("file uploaded to s3")
	return nil
}

func putWork(line string, add string, s3 bool) {
	fname := line[1:len(line)]
	log.Debugln("http://" + add + "/nitro-junk/" + fname)
	if s3 {
		s3Upload("nitro-junk", fname, line)
	} else {
		data, err := os.Open(line)
		if err != nil {
			log.Fatal(err)
		}
		//log.Debugln("PUT File"+line, add)
		defer data.Close()
		resp, err := http.Post("http://"+add+"/nitro-junk"+fname, "text/plain", data)
		if err != nil {
			log.Errorln(err)
		} else {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
			log.Debugln("Req Method:", resp.Request.Method)
		}

		return
	}
}

func getWork(line string, add string) {
	fname := line[1:len(line)]
	log.Debugln("http://" + add + "/nitro-junk/" + fname)
	resp, err := http.Get("http://" + add + "/nitro-junk" + fname)
	if err != nil {
		log.Errorln("Error downloading %s", line)
	} else {
		log.Debugln("Req Method:", resp.Request.Method)
		resp.Body.Close()
	}
	return
}

func worker(add string, id int, results chan<- int) {
	log.Debugln("Worker  Number", id)
	os.Chdir("/Users/bparli/tests/")
	// Open the file.
	f, _ := os.Open("/Users/bparli/tests/tests.txt")
	// Create a new Scanner for the file.
	scanner := bufio.NewScanner(f)
	// Loop over all lines in the file and print them.
	count := 1
	s3 := false

	if id == 12 || id == 14 {
		for scanner.Scan() {
			line := scanner.Text()
			putWork(line, add, s3)
		}
	} else {
		for scanner.Scan() {
			line := scanner.Text()
			if count%2 == 0 {
				putWork(line, add, s3)
				getWork(line, add)
			} else {
				getWork(line, add)
			}
			count++
		}
		results <- 1
	}

	return
}

func main() {
	log.SetLevel(log.DebugLevel)
	numThreads, _ := strconv.Atoi(os.Args[1])

	results12 := make(chan int, 1)
	results14 := make(chan int, 1)
	//go worker("10.20.20.119:8081", 12, results12)
	//go worker("172.16.46.180:8082", 14, results14)
	go worker("s3-us-west-1.amazonaws.com", 12, results12)
	go worker("s3-us-west-1.amazonaws.com", 14, results14)

	time.Sleep(5 * time.Second)

	results1 := make(chan int, numThreads)
	results2 := make(chan int, numThreads)

	start := time.Now()
	for w := 0; w < numThreads; w++ {
		log.Debugln("Main Worker  Number", w)
		//go worker("10.20.20.119:8081", w, results1)
		//go worker("172.16.46.180:8082", w, results2)
		go worker("s3-us-west-1.amazonaws.com", w, results1)
		go worker("s3-us-west-1.amazonaws.com", w, results2)
	}

	//<-results12
	//<-results14

	elapsed := time.Since(start)
	for a := 0; a < numThreads; a++ {
		<-results1
		log.Debugln("results1 are back")
		elapsed = time.Since(start)
		log.Debugln("Elapsed Time: a", elapsed)
		<-results2
		log.Debugln("results2 are back")
		elapsed = time.Since(start)
		log.Debugln("Elapsed Time: a", elapsed)
	}
	elapsed = time.Since(start)
	log.Debugln("Done!")
	log.Debugln("Final Elapsed Time:", elapsed)
}
