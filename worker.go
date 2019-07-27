package main

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)
const(							//WORKER_ISTANCE_STATES, NB not all workers will execute all these states
	IDLE	int=iota
	MAP_EXEC
	WAITING_REDUCERS_ADDRESSES
	REDUCE_EXEC
	FAILED
)
/////////		WORKER STRUCTS		//////
///MASTER SIDE STRUCTS
type WorkersKinds struct{				//refs to up&running workers
	workersMapReduce 	[]Worker		//mutable worker
	workersOnlyReduce 	[]Worker		//separated worker for reduce
	workersBackup 		[]Worker		//idle workers for backup
}


type Worker struct {																	//worker istance master side
	address string
	id 		int
	state   WorkerStateMasterControl

}
type WorkerStateMasterControl struct {
	//struct for all data that a worker node may have during computation
	//because a worker may or may not become both mapper and reducer not all these filds will be used
	chunksIDs []int									//chunks located on Worker
	/*
		ids of worker istances --> rpc port
		for each worker istance running on worker node there is a separated rpc server
		listening for request on individual ports
	*/
	workerIstances	 map[int] _workerIstanceControl	//for each worker istance ID -> control info (port,state,..)
	chunkServIstance _workerIstanceControl
	workerNodeLinks	 map[int] *Worker				//link each worker istance ID to worker node that contains it
}


///WORKER ISTANCE SIDE		routine-task-~-container adaptable
type MapperIstanceStateInternal struct {
	//// internal workerstates that are unknown to the master
	intermediateTokens map[string]int				//produced by map, routed to reducer
}
type ReducerIstanceStateInternal struct {
	//// internal workerstate, almost unknown to the master
	//reduce
	intermediateTokenShares	[]map[string]int		//mappers intermediate tokens shares received (needed for better fault tollerance
	intermediateTokensCumulative map[string]int		//used by reduce calls to aggregate intermediate tokens from the map executions
	cumulativeCalls	int								//cumulative number of reduce calls received
	expectedRedCalls	int							//expected num of reduce calls from mappers	(received from the master)
}
type _GenericInternalState struct {
	mapData	MapperIstanceStateInternal
	reduceData	ReducerIstanceStateInternal
}
type _workerIstanceControl struct{
	port    int
	kind    int                    //RPC ISTANCE TYPE CODE
	intState int 				   //internal state of the istance
	client  *rpc.Client				//not nil only at master

}
type _workerIstanceInternal struct{
	server  *rpc.Server
	controlData _workerIstanceControl
	intData _GenericInternalState  //generic data carried by istance discriminated by kind //TODO REDUNDANT OTHER THAN intState=?
}

///WORKER SIDE STRUCTS
var Worker_node_internal struct { //global worker istance worker node side
	workerChunksStore 	WorkerChunks				//chunks stored in the worker node
	server map[int]		_workerIstanceInternal		//server for each worker istance id
}
type WorkerChunks struct{ 		//WORKER NODE LEVEL STRUCT
	//HOLD chunks stored in worker node pretected by a mutex
	mutex sync.Mutex			//protect concurrent access to chunks
	chunks map[int]CHUNK		//chunks stored by id in worker

}

////WORKERS UTIL FUNCTIONS
func workerNodeWithMapper(mapperID int) (*Worker,error) { //MASTER SIDE
	//return worker istance ref containing mapper with mapperID among master ref to worker istances
	var workerNodeWithMapperIstance *Worker
	var present bool
	for _,worker :=range Workers.workersMapReduce{	//find worker node
		workerNodeWithMapperIstance,present=worker.state.workerNodeLinks[mapperID]
	}
	for _,worker :=range Workers.workersBackup{	//find worker node
		workerNodeWithMapperIstance,present=worker.state.workerNodeLinks[mapperID]
	}

	if !present{
		return	nil,errors.New("NOT FOUNDED WORKER NODE WITH MAP ISTANCE WITH ID "+strconv.Itoa(mapperID))
	}
	return workerNodeWithMapperIstance,nil
}
func numHealthyReducerOnWorker(workerNode *Worker) int{
	//return number of reducer istances healthy  on workerNOde
	numHealthyReducers:=0
	for _,istanceState:=range workerNode.state.workerIstances{
		if istanceState.intState!=FAILED {
			numHealthyReducers++
		}
	}
	return numHealthyReducers
}

//INIT RPC SERVICE CODES to register different RPC
const(
	CHUNK_ID_INIT		int=iota				//chunk service running on all workers
	MAP
	//REDUCER_BINDINGS	//included in map (thread safe because comunicated at the end of all MAP() op
	REDUCE
	//PRE_REDUCER_INIT		//included in reduce worker istance
	BACKUP
	CONTROL
	MASTER

)
func initRPCWorkerIstance(istanceId int,port int,kind int){
	//Create an instance of structs which implements map and reduce interfaces
	//init code used to discriminate witch rpc to activate (see costants)
	var err error
	server := rpc.NewServer()
	workerIstanceData:=_workerIstanceInternal{
		server:   server,
		controlData:     _workerIstanceControl{intState:IDLE,port:port},
	}
	if kind==MAP {
		map_ := new(mapper)
		err = server.RegisterName("MAP", map_)
		workerIstanceData.intData.mapData=MapperIstanceStateInternal(*map_)
	} else if kind ==REDUCE{
		reduce_ := new(reducer)
		err = rpc.RegisterName("REDUCE", reduce_)
		workerIstanceData.intData.reduceData=ReducerIstanceStateInternal(*reduce_)
	}	else if kind ==CHUNK_ID_INIT{
		workersChunks := new(workerChunk)
		Worker_node_internal.workerChunksStore=WorkerChunks(*workersChunks)
		err= rpc.RegisterName("CHUNK",workersChunks)
	}	else {
		panic("unrecognized code"+strconv.Itoa(kind))
	}
	checkErr(err,true,"Format of service selected  is not correct: ")
	workerIstanceData.server=server
	workerIstanceData.controlData=_workerIstanceControl{
		port:     port,
		kind:     kind,
		intState: IDLE,
	}
	Worker_node_internal.server[istanceId]= workerIstanceData
	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	checkErr(e,true,"listen err on port "+strconv.Itoa(port))
	go pingHeartBitRcv()
	server.Accept(l) //TODO 4EVER BLOCK-> EXIT CONDITION DEFINE see under
	//_:=l.Close()           //TODO will unblock rpc requests handler routine
	//runtime.Goexit()    	//routine end here
}

const CHUNK_INIT_BASE_PORT=	7777	//base port for chunk get service (default on all workers)
func main() {
	//INIT AN RPC SERVER listening from passed port
	//END RPC SERVER ON MASTER NOTIFY ON DONE CHANNEL
	errs:=make([]error,3)
	var err error
	id,err := strconv.Atoi(os.Args[1])
	errs=append(errs,err)
	port,err := strconv.Atoi(os.Args[2])
	errs=append(errs,err)
	rpcCODE,err := strconv.Atoi(os.Args[3])
	errs=append(errs,err)

	checkErrs(errs,true,"initialize a new worker with a base worker istance running on him USAGE <ID,port,RPC_kind_code> ")
	initRPCWorkerIstance(id,CHUNK_INIT_BASE_PORT,CHUNK_ID_INIT)
	initRPCWorkerIstance(id,port,rpcCODE)
}