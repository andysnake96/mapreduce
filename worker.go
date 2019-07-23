package main

import (
	"net"
	"net/rpc"
	"os"
	"strconv"
)
const(							//WORKER_STATES, NB not all workers will execute all these states
	IDLE	int=iota
	MAP
	WAITING_REDUCERS_ADDRESSES
	REDUCE
	FAILED
)

type WorkerstateExternal struct {
	//state of a worker known to the master, obtained from data passed from the master
	chunksIDs []int									//chunks located on Worker
	state	int										//one in WORKER_STATES
	expectedCalls	int								//expected num of reduce calls from mappers
}
type WorkerstateInternal struct {
	//// internal workerstate, unknown to the master
	//map
	intermediateTokens map[string]int				//produced by map, routed to reducer
	//reduce
	intermediateTokensCumulative map[string]int		//used by reduce calls to aggregate intermediate tokens from the map executions
	cumulativeCalls	int								//cumulative number of reduce calls received
}
type Workerstate struct {
	//struct for all data that a worker may have during map and reduce
	//because a worker may or may not become both mapper and reducer not all these filds will be used
	workerStateExternal WorkerstateExternal
	workerStateInternal WorkerstateInternal			//nil at the master

}
var WorkerStateActual Workerstate //state ref for a worker
type Worker struct {
	port    int
	address string
	state   Workerstate
	client  *rpc.Client
}
var Chunks []CHUNK

/*
single Worker initialization module
 */
func initRPC(port int){
	//Create an instance of structs which implements map and reduce interfaces
	map_ := new(_map)
	reduce_ := new(_reduce)

	//REGISTER MAP AND REDUCE METHODS
	server := rpc.NewServer()

	err := server.RegisterName("Map", map_)
	checkErr(err,false,"Format of service Map is not correct: ")
	err = server.RegisterName("Reduce", reduce_)
	checkErr(err,false,"Format of service Reduce is not correct: ")
	// TODO FAULT TOLLERANT STATE SERIALIZZATION
	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	checkErr(e,true,"listen err on port "+strconv.Itoa(port))
	go pingHeartBitRcv()
	server.Accept(l) //TODO 4EVER BLOCK-> EXIT CONDITION DEFINE see under
	//_:=l.Close()           //TODO will unblock rpc requests handler routine
	//runtime.Goexit()    	//routine end here
}
func main() {
	//INIT AN RPC SERVER listening from passed port
	//END RPC SERVER ON MASTER NOTIFY ON DONE CHANNEL
	port,err := strconv.Atoi(os.Args[1])
	checkErr(err,true,"USAGE <port> ")
	initRPC(port)
}