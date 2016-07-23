# s3envoy
S3Envoy is an S3 Proxy layer to cache objects local to the application's environment but still ultimately backed by S3

##Motivation
As a Smart Docs Company, Nitro uses S3 quite a bit and for a variety of purposes.  But S3 operations are relatively heavy operations and might not be the best design in some cases.  For Nitro's Hack Week I hypothesized performance could be improved simply by keeping recently used objects/files warm in a more local store instead of sending all the way to the AWS S3 service.  Part of the logic behind this reasoning also has to do with behavior and usage patterns; files/objects which are PUT are recently are more likely to be retrieved in the near future.  Despite all this, these objects still should be ultimately backed by S3, but we can use non-blocking helper threads for that.

##Installation
To install simply clone the project into your Go enviroment and run "go install s3envoy/s3proxy"

Prerequisites include:
* [Hashicorp's memberlist library ](https://github.com/Nitro/memberlist)
* [The Gorrila mux router](https://github.com/gorilla/mux)
* [AWS SDK](https://github.com/aws/aws-sdk-go)
* [Pivotal's byte converter](github.com/pivotal-golang/bytefmt)

##S3Envoy 
The idea behind S3Envoy is to provide a HTTP-based service just like S3 so clients will not need to be altered.  The service accepts GET/PUT requests just like S3.  It uses an LRU queue to maintain the cache and helper threads to handle background interfacing with S3 itself.  Objects are stored on disk for persistence and in-memory for fast retrieval.

<img src="https://github.com/bparli/s3envoy/blob/master/png/S3Envoy.png" width="200" height="250">

###Cluster Mode
A cluster mode setting is also available in which S3Envoy peers maintain their own view of a global hash table.   
The Global hash Table is used redirect requests to peers if they are able to service a request from their local store.  So each server keeps its local LRU Queue in addition to its view of the Global Hash Table

###Other Settings
S3Envoy can be tuned via a config.json file.  Additional parameters include memory settings, maximum file size to keep in memory, maximum disk capacity, and the list of Peers.

##GET Example
1. Check LRU Queue – serve if found and move to head
2. Check local Global Hash Table – redirect if found
3. If not found GET from AWS S3 
4. Store locally, update local LRU Queue, local view of Global Hash Table
5. Helper thread to notify peers of update to Global Hash
 
<img src="https://github.com/bparli/s3envoy/blob/master/png/GET.png" width="200" height="250">

##PUT Example
1. Store locally – if small enough for in-mem, and also on disk (persistent)
2. Update LRU Queue – move to head
3. Update local Global Hash Table and send update to peers (helper thread)
4. PUT to S3 with Helper Thread

<img src="https://github.com/bparli/s3envoy/blob/master/png/PUT.png" width="200" height="250">

##Takeways
To verify the initial motivation, some experiements were run with a hacked together client load testing program (to install this package simply run "go install s3envoy/client" from your go environment).  This program performed GETs and PUTs from a directory of 4300+ files of various types (jars, yml, pdf, txt, etc) and sizes (avg: 650 KB, max: 635 MB, Min: 2 KB).  First prime the pump a bit; 2 Threads with continuous PUT Requests.  Then in a 2:1 GET:PUT ratio, series of requests from 2, 4, and 6 worker threads.  Results of this toy experiemnt are below.

| #S3Envoy Processes  | #Load Worker Threads  | S3Envoy Completion Time (S) |      S3 Completion Time (S) | % Improvement |
|:-------------------:|:---------------------:|:---------------------------:|:---------------------------:|:-------------:|
| 2                   |  2                    |   63                        |         189                 |    66%        |
| 2                   |  4                    |   58                        |         210                 |    72%        |
| 2                   |  6                    |    97                       |         221                 |    56%        |
