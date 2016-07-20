package main

import (
	"bufio"
	"net/http"
	"os"
	"strconv"

	log "github.com/Sirupsen/logrus"
)

func putWork(line string, add string) {
	data, err := os.Open(line)
	if err != nil {
		log.Fatal(err)
	}
	//log.Debugln("PUT File"+line, add)
	defer data.Close()
	fname := line[1:len(line)]
	log.Debugln("http://" + add + "/nitro-junk/" + fname)
	// req, err := http.NewRequest("PUT", "http://"+add+"/nitro-junk/"+fname, data)
	// if err != nil {
	// 	log.Errorln(err)
	// }
	// log.Debugln("Req Method:", req.Method)
	//
	// client := &http.Client{}
	// res, err := client.Do(req)
	// defer res.Body.Close()
	// if err != nil {
	// 	log.Errorln(err)
	// }
	resp, err := http.Post("http://"+add+"/nitro-junk"+fname, "text/plain", data)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

}

func getWork(line string, add string) {
	fname := line[1:len(line)]
	log.Debugln("http://" + add + "/nitro-junk/" + fname)
	resp, err := http.Get("http://" + add + "/nitro-junk" + fname)
	log.Debugln("Req Method:", resp.Request.Method)
	if err != nil {
		log.Errorln("Error downloading %s", line)
	}
	resp.Body.Close()
}

func worker(add string, id int, results chan<- int) {
	log.Debugln("Worker  Number %d \n", id)
	os.Chdir("/Users/bparli/tests/")
	// Open the file.
	f, _ := os.Open("/Users/bparli/tests/tests.txt")
	// Create a new Scanner for the file.
	scanner := bufio.NewScanner(f)
	// Loop over all lines in the file and print them.
	count := 1
	for scanner.Scan() {
		line := scanner.Text()
		if count%2 == 0 {
			putWork(line, add)
		} else {
			getWork(line, add)
		}
		count++
	}
	results <- 1
}

func main() {
	log.SetLevel(log.DebugLevel)
	numThreads, _ := strconv.Atoi(os.Args[1])

	results1 := make(chan int, numThreads)
	//results2 := make(chan int, numThreads)

	for w := 1; w <= numThreads; w += 1 {
		log.Debugln("Main Worker  Number %d \n", w)
		go worker("10.20.20.119:8081", w, results1)
		//go worker("172.16.46.180:8082", w+1, results2)
	}

	for a := 1; a <= numThreads; a++ {
		<-results1
		//<-results2
	}
	log.Debugln("Done!")
}
