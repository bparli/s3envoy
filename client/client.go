package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
)

func putWork(line string) {
	data, err := os.Open(line)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("PUT File" + line)
	defer data.Close()
	fname := line[1:len(line)]
	req, err := http.NewRequest("PUT", "http://localhost:8080/nitro-junk"+fname, data)
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Body.Close()
}

func getWork(line string) {
	fname := line[1:len(line)]
	fmt.Print("GET File" + line)
	_, err := http.Get("http://localhost:8080/nitro-junk" + fname)
	if err != nil {
		fmt.Printf("Error downloading %s", line)
	}
}

func worker(id int, results chan<- int) {
	fmt.Printf("Worker  Number %d \n", id)
	os.Chdir("/Users/bparli/tests/")
	// Open the file.
	f, _ := os.Open("/Users/bparli/tests/tests.txt")
	// Create a new Scanner for the file.
	scanner := bufio.NewScanner(f)
	// Loop over all lines in the file and print them.
	count := 1
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
		if count%2 == 0 {
			getWork(line)
		} else {
			putWork(line)
		}
	}
	results <- 1
}

func main() {
	numThreads, _ := strconv.Atoi(os.Args[1])

	results := make(chan int, numThreads)

	for w := 1; w <= numThreads; w++ {
		fmt.Printf("Main Worker  Number %d \n", w)
		go worker(w, results)
	}

	for a := 1; a <= numThreads; a++ {
		<-results
	}
	fmt.Println("Done!")
}
