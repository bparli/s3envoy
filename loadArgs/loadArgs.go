package loadArgs

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/pivotal-golang/bytefmt"
)

//Args struct to read config file and set global vars
type Args struct {
	LocalPath      string
	TotalFiles     int
	MemCap         int64
	DiskCap        int64
	MaxMemFileSize int64
	Peers          []string
	LocalName      string
	Cluster        bool
	HashPort       string
}

type argsInput struct {
	LocalPath      string   `json:"LocalPath"`
	TotalFiles     string   `json:"TotalFiles"`
	MemCap         string   `json:"MemCap"`
	DiskCap        string   `json:"DiskCap"`
	MaxMemFileSize string   `json:"MaxMemFileSize"`
	LocalName      string   `json:"LocalName"`
	Cluster        string   `json:"Cluster"`
	HashPort       string   `json:"HashPort"`
	Peers          []string `json:"Peers"`
	// Nodes          struct {
	// 	Peer1 string `json:"Node1"`
	// 	Peer2 string `json:"Node2"`
	// } `json:"Nodes"`
}

//Load function to load config file and return struct with args
func Load(conf string) *Args {

	configFile, err := os.Open(conf)
	defer configFile.Close()
	if err != nil {
		log.Fatal(err)
	}
	args := new(argsInput)
	json.NewDecoder(configFile).Decode(args)

	var localPath string
	var totalFiles int
	var memCap string
	var diskCap string
	var maxMemFileSize string
	//var peers []string
	var cluster bool
	var hashPort string
	var localName string

	if args.LocalPath == "" {
		localPath = "/Users/bparli/tmp/"
	} else {
		localPath = args.LocalPath + "/"
	}

	if args.TotalFiles == "" {
		totalFiles = 10
	} else {
		qsize, _ := strconv.Atoi(args.TotalFiles)
		totalFiles = qsize
	}

	if args.LocalName == "" {
		localName = "127.0.0.1:9081"
	} else {
		localName = args.LocalName
	}

	if args.MemCap == "" {
		memCap = "100M"
	} else {
		memCap = args.MemCap
	}
	memCap2, _ := bytefmt.ToBytes(memCap)

	if args.DiskCap == "" {
		diskCap = "500M"
	} else {
		diskCap = args.DiskCap
	}
	diskCap2, _ := bytefmt.ToBytes(diskCap)

	if args.MaxMemFileSize == "" {
		maxMemFileSize = "10M"
	} else {
		maxMemFileSize = args.MaxMemFileSize
	}
	maxMemFileSize2, _ := bytefmt.ToBytes(maxMemFileSize)

	if args.HashPort == "" {
		hashPort = "9081"
	} else {
		hashPort = args.HashPort
	}

	if args.Cluster == "" || args.Cluster == "False" {
		cluster = false
		//peers = []string{}
	} else if args.Cluster == "True" {
		cluster = true
	}

	new := &Args{LocalPath: localPath,
		TotalFiles: totalFiles, MemCap: int64(memCap2),
		DiskCap: int64(diskCap2), MaxMemFileSize: int64(maxMemFileSize2),
		Peers: args.Peers, LocalName: localName, Cluster: cluster,
		HashPort: hashPort}

	fmt.Println("Config file Args:", new)
	return new
}
