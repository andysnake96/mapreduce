package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strconv"
	"syscall"
)

/*
Worker MODULE
Worker will be bounded to different ports by adding a costant to Configuration.PORTBASE
rpc servers will be rysed on different processes
will be rysed the max from rpc server for map and reduce
*/



func workersInit(n int) []Worker {
	//init n Worker and return a Worker struct for each rysed Worker
	workerRefs := make([]Worker, n)
	//var err error
	for x := 0; x < n; x++ {
		port := Configuration.PORTBASE + x
		workerRefs[x].port = port
		address := fmt.Sprint("localhost:", port)
		workerRefs[x].address = address
		//syscall.ForkExec()	TODO POSSIBLE FOR RUN Worker CODE for each Worker needed to be spawned
		//workerRefs[x].client, err = rpc.Dial("tcp", address)
		//checkErr(err,true,"")
	}
	return workerRefs
}
