.ONESHELL:
SHELL=/bin/bash
build:
	GOPATH+= $$(realpath .)
	go build
	echo $$GOPATH
downloadtxts:
	cd txtSrc
	for url in $$(cat urlSources.txt); do wget $$url;done 


