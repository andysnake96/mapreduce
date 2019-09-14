.ONESHELL:
SHELL=/bin/bash
build:
	GOPATH+= $$(realpath .)
	echo $$GOPATH
	go get  github.com/aws/aws-sdk-go/aws
	go get  github.com/aws/aws-sdk-go/service/s3
	go get  golang.org/x/text/encoding/unicode

worker: build
	cd worker
	go build

master: build
	cd master
	go build

