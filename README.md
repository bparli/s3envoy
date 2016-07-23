# s3envoy
S3Envoy is an S3 Proxy layer to cache objects local to the application's environment but still ultimately backed by S3

##Motivation


##Installation
To install simply clone the project into your Go enviroment and run "go install s3envoy/s3proxy"

Prerequisites include:
* [Hashicorp's memberlist library ](https://github.com/Nitro/memberlist)
* [The Gorrila mux router](https://github.com/gorilla/mux)
* [AWS SDK](https://github.com/aws/aws-sdk-go)
* [Pivotal's byte converter](github.com/pivotal-golang/bytefmt)
