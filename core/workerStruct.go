package core

import (
	"net"
	"net/rpc"
	"strconv"
	"sync"
)

const WORKER_CHUNKS_INITSIZE_DFLT = 3

type CHUNK string

/////////		WORKER STRUCTS		//////
///MASTER SIDE STRUCTS
type WorkersKinds struct { //refs to up&running workers
	WorkersMapReduce  []Worker //mutable worker
	WorkersOnlyReduce []Worker //separated worker for reduce
	WorkersBackup     []Worker //idle workers for backup
}
type Worker struct { //worker istance master side
	Address string
	Id      int
	State   WorkerStateMasterControl
}
type WorkerStateMasterControl struct {
	//struct for all data that a worker node may have during computation
	//because a worker may or may not become both Mapper and Reducer not all these filds will be used
	ChunksIDs []int //all chunks located on Worker
	//ChunksIDsFairShare []int //share of chunks located on Worker needed for map
	/*
		ids of worker istances --> rpc port
		for each worker istance running on worker node there is a separated rpc server
		listening for request on individual ports
		ports will change in different port spaces defined in  config file,
		on reassignement new port will be calculated watching instances max port by kind
	*/
	WorkerIstances     map[int]WorkerIstanceControl //for each worker istance ID -> control info (port,state,..)
	ControlRPCInstance WorkerIstanceControl         //chunkService worker node level instance
	WorkerNodeLinks    *Worker                      //backward link it
}

///WORKER ISTANCE SIDE		routine-task-~-container adaptable
type MapperIstanceStateInternal struct {
	IntermediateTokens map[string]int //produced by map, routed to Reducer
	WorkerChunks       *WorkerChunks  //readonly ref to chunks stored in worker that contains this Mapper istance
	ChunkID            int            //assigned ChunkID unique identifier for a MAPPER work result
}

type ReducerIstanceStateInternal struct {
	//// internal workerstate, almost unknown to the master
	IntermediateTokenShares      map[string]int //mappers intermediate tokens shares received (needed for better fault tollerance
	IntermediateTokensCumulative map[string]int //used by reduce calls to aggregate intermediate tokens from the map executions
	CumulativeCalls              map[int]int    //cumulative number of reduce calls received
	ExpectedRedCalls             map[int]int    //expected num of reduce calls from mappers	(hashed by their assigned chunk)
	mutex                        sync.Mutex     //because of multiple calls it protect cumulatives fields changes
}

type GenericInternalState struct {
	MapData    MapperIstanceStateInternal
	ReduceData ReducerIstanceStateInternal
}
type WorkerIstanceControl struct {
	Id       int
	Port     int
	Kind     int         //RPC ISTANCE TYPE CODE
	IntState int         //internal state of the istance
	Client   *rpc.Client //not nil only at master

}
type WorkerInstanceInternal struct {
	Server      *rpc.Server
	ControlData WorkerIstanceControl
	IntData     GenericInternalState //generic data carried by istance discriminated by kind //TODO REDUNDANT OTHER THAN intState=?
}

//WORKER_ISTANCE_STATES, NB not all istances will execute all these states
const (
	IDLE int = iota
	MAP_EXEC
	WAITING_REDUCERS_ADDRESSES
	REDUCE_EXEC
	FAILED
)

//INIT RPC SERVICE CODES to register different RPC
const (
	CONTROL int = iota //control services
	MAP
	REDUCE
	MASTER
)

///WORKER SIDE STRUCTS
type Worker_node_internal struct { //global worker istance worker node side
	WorkerChunksStore  WorkerChunks                   //chunks stored in the worker node
	Instances          map[int]WorkerInstanceInternal //server for each worker istance Id
	ControlRpcInstance WorkerInstanceInternal         //worker node main control instace ->chunks&&respawn
	ReducersClients    []rpc.Client
}
type WorkerChunks struct { //WORKER NODE LEVEL STRUCT
	//HOLD chunks stored in worker node pretected by a mutex
	Mutex  sync.Mutex    //protect concurrent access to chunks
	Chunks map[int]CHUNK //chunks stored by Id in worker

}

////init worker referements master
func InitWorkerInstancesRef(worker *Worker, port int, istanceType int) (WorkerIstanceControl, error) {
	//init a worker istance referement to the master of istanceType  behind a port
	// return newly created instance ref and eventual error of client estamblishment
	//find max assigned Id for new instanceId
	maxId := 0
	for id, _ := range worker.State.WorkerIstances {
		if id > maxId {
			maxId = id
		}
	}
	maxId++ //new worker unique Id for the new instance
	println("init new instance on worker: ", worker.Id, "with Id: ", maxId)
	client, err := rpc.Dial(Config.RPC_TYPE, worker.Address+":"+strconv.Itoa(port))
	var newWorkerInstance WorkerIstanceControl
	if !CheckErr(err, false, "init worker") {
		newWorkerInstance = WorkerIstanceControl{
			Port:     port,
			Kind:     istanceType,
			IntState: IDLE,
			Client:   client,
			Id:       maxId,
		}
		if istanceType == CONTROL {
			worker.State.ControlRPCInstance = newWorkerInstance
		} else {
			worker.State.WorkerIstances[maxId] = newWorkerInstance
		}
	}
	return newWorkerInstance, err
}

func InitRPCWorkerIstance(initData *GenericInternalState, port int, kind int, workerNodeInt *Worker_node_internal) (error, int) {
	//Create an instance of structs which implements map and reduce interfaces
	//init code used to discriminate witch rpc to activate (see costants)
	//initialization data for the new instance can be provided with parameter initData filling the right field
	//return error or new ID for the created instance
	var err error
	maxId := GetMaxIdWorkerInstances(&workerNodeInt.Instances) //find new unique ID
	newId := maxId + 1

	workerIstanceData := WorkerInstanceInternal{
		Server: rpc.NewServer(),
		ControlData: WorkerIstanceControl{
			Id:       newId,
			Port:     port,
			Kind:     kind,
			IntState: IDLE,
		}}
	if kind == MAP {
		mapper := new(Mapper)
		if initData != nil {
			mapper = (*Mapper)(&(initData.MapData))
		}
		err = workerIstanceData.Server.RegisterName("MAP", mapper)
		mapper.WorkerChunks = &workerNodeInt.WorkerChunksStore //link to chunks for mapper
		workerIstanceData.IntData.MapData = MapperIstanceStateInternal(*mapper)
	} else if kind == REDUCE {
		reducer := new(Reducer)
		if initData != nil {
			reducer = (*Reducer)(&(initData.ReduceData))
		}
		err = workerIstanceData.Server.RegisterName("REDUCE", reducer)
		workerIstanceData.IntData.ReduceData = ReducerIstanceStateInternal(*reducer)
	} else if kind == CONTROL { //instances for controlRPCs and ChunksService at workerNodeLevel
		//chunk service
		workerNodeInt.WorkerChunksStore.Chunks = make(map[int]CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		err = workerIstanceData.Server.RegisterName("CONTROL", workerNodeInt)
		workerNodeInt.ControlRpcInstance = workerIstanceData
	} else {
		panic("unrecognized code" + strconv.Itoa(kind))
	}
	if CheckErr(err, false, "Format of service selected  is not correct: ") {
		return err, -1
	}

	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	if CheckErr(e, false, "listen err on port "+strconv.Itoa(port)) {
		return e, -1
	}
	//go PingHeartBitRcv()
	go workerIstanceData.Server.Accept(l) //TODO 4EVER BLOCK-> EXIT CONDITION DEFINE see under
	//_:=l.Close()           	//TODO will unblock rpc requests handler routine
	//runtime.Goexit()    		//routine end here
	return nil, maxId
}
