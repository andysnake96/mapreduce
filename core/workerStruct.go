package core

import (
	"net/rpc"
	"strconv"
	"sync"
)

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
	ChunksIDs []int //chunks located on Worker
	/*
		ids of worker istances --> rpc port
		for each worker istance running on worker node there is a separated rpc server
		listening for request on individual ports
		ports will change in different port spaces defined in  config file,
		on reassignement new port will be calculated watching instances max port by kind
	*/
	WorkerIstances   map[int]WorkerIstanceControl //for each worker istance ID -> control info (port,state,..)
	ChunkServIstance WorkerIstanceControl
	WorkerNodeLinks  map[int]*Worker //link each worker istance ID to worker node that contains it
}

///WORKER ISTANCE SIDE		routine-task-~-container adaptable
type MapperIstanceStateInternal struct {
	//// internal workerstates that are unknown to the master
	IntermediateTokens map[string]int //produced by map, routed to Reducer
	workerChunks       *WorkerChunks  //readonly ref to chunks stored in worker that contains this Mapper istance
}
type ReducerIstanceStateInternal struct {
	//// internal workerstate, almost unknown to the master
	//reduce
	IntermediateTokenShares      []Token        //mappers intermediate tokens shares received (needed for better fault tollerance
	IntermediateTokensCumulative map[string]int //used by reduce calls to aggregate intermediate tokens from the map executions
	CumulativeCalls              int            //cumulative number of reduce calls received
	ExpectedRedCalls             int            //expected num of reduce calls from mappers	(received from the master)
}
type GenericInternalState struct {
	MapData    MapperIstanceStateInternal
	ReduceData ReducerIstanceStateInternal
}
type WorkerIstanceControl struct {
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
	CHUNK_ID_INIT int = iota //chunk service running on all workers
	MAP
	//REDUCER_BINDINGS	//included in map (thread safe because comunicated at the end of all MAP() op
	REDUCE
	//PRE_REDUCER_INIT		//included in reduce worker istance
	BACKUP
	CONTROL
	MASTER
)

///WORKER SIDE STRUCTS
type Worker_node_internal struct { //global worker istance worker node side
	WorkerChunksStore WorkerChunks                   //chunks stored in the worker node
	Server            map[int]WorkerInstanceInternal //server for each worker istance Id
}
type WorkerChunks struct { //WORKER NODE LEVEL STRUCT
	//HOLD chunks stored in worker node pretected by a mutex
	Mutex  sync.Mutex    //protect concurrent access to chunks
	Chunks map[int]CHUNK //chunks stored by Id in worker

}

////init worker referements master

func initWorkerInstancesRef(worker *Worker, port int, istanceID, istanceType int) {
	//init a worker istance referement to the master of istanceType  behind a port

	//init chunk service istance for every worker
	client, err := rpc.Dial(Config.RPC_TYPE, worker.Address+":"+strconv.Itoa(port))
	CheckErr(err, true, "init worker")
	worker.State.WorkerIstances[istanceID] = WorkerIstanceControl{
		Port:     Config.CHUNK_SERVICE_BASE_PORT,
		Kind:     CHUNK_ID_INIT,
		IntState: IDLE,
		Client:   client,
	}
	worker.State.WorkerNodeLinks[istanceID] = worker //quick backward link

}
func InitWorkers() WorkersKinds {
	/*
		init all kind of workers with addresses and base   Config
		dinamically from generated addresses file at init phase
		explicit init of worker by kind for future change
	*/
	var worker Worker
	workersOut := *new(WorkersKinds)
	idWorker := 0
	workersOut.WorkersMapReduce = make([]Worker, len(Addresses.WorkersMapReduce))
	for i, address := range Addresses.WorkersMapReduce {
		id := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}
		initWorkerInstancesRef(&worker, Config.CHUNK_SERVICE_BASE_PORT, id, CHUNK_ID_INIT)
		id++
		initWorkerInstancesRef(&worker, Config.MAP_SERVICE_BASE_PORT, id, MAP)
		id++
		workersOut.WorkersMapReduce[i] = worker
		idWorker++
	}
	workersOut.WorkersOnlyReduce = make([]Worker, len(Addresses.WorkersOnlyReduce))
	for i, address := range Addresses.WorkersOnlyReduce {
		id := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}
		initWorkerInstancesRef(&worker, Config.CHUNK_SERVICE_BASE_PORT, id, CHUNK_ID_INIT)
		id++
		initWorkerInstancesRef(&worker, Config.MAP_SERVICE_BASE_PORT, id, REDUCE)
		workersOut.WorkersOnlyReduce[i] = worker
		idWorker++
	}
	workersOut.WorkersBackup = make([]Worker, len(Addresses.WorkersBackup))
	for i, address := range Addresses.WorkersBackup {
		id := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}
		initWorkerInstancesRef(&worker, Config.CHUNK_SERVICE_BASE_PORT, id, CHUNK_ID_INIT)
		id++
		workersOut.WorkersBackup[i] = worker
		idWorker++
	}
	return workersOut
}
