package core

import (
	"net"
	"net/rpc"
	"strconv"
	"sync"
)

const WORKER_CHUNKS_INITSIZE_DFLT = 3

type CHUNK string

const ( //WORKERS KINDS
	WORKERS_MAP_REDUCE  = "MAP_REDUCE"
	WORKERS_ONLY_REDUCE = "ONLY_REDUCE"
	WORKERS_BACKUP_W    = "WORKERS_BACKUP"
)

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
	WorkerIstances     map[int]WorkerIstanceControl //for each worker istance ID -> control info (port,state,internalData)
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
	IntermediateTokensCumulative map[string]int //used by reduce calls to aggregate intermediate tokens from the map executions

	//cumulative reduce calls per chunkID from mappers...(initiated from master)
	//used as exit condition on all true and also easily support mapper fault recovery on other instances
	//TODO on reduce calls produced by aggregation of MAP(chunks) expected no partial collision on new chunk's derived intermediate data and old one already reduced
	//TODO MASTER SMART REASSIGN
	CumulativeCalls map[int]bool //cumulative number of reduce calls received per chunk ID (for exit condition) (
	mutex           sync.Mutex   //protect cumulative vars from concurrent access
	MasterClient    *rpc.Client  //to report back to master final token share of reducer
}

type GenericInternalState struct { //worker instance generic internal state ( field to use is discriminable from instance kind filed)
	MapData    MapperIstanceStateInternal
	ReduceData ReducerIstanceStateInternal
}
type WorkerIstanceControl struct { //MASTER SIDE instance
	Id       int
	Port     int
	Kind     int
	IntState int         //internal state of the istance
	Client   *rpc.Client //not nil only at master

}
type WorkerInstanceInternal struct {
	Server      *rpc.Server //TODO OLD
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

type AggregatedIntermediateTokens struct {
	ChunksSouces                 []int
	PerReducerIntermediateTokens []map[string]int
}

///WORKER SIDE STRUCTS
type Worker_node_internal struct {
	//global worker istance worker node side
	WorkerChunksStore          WorkerChunks //chunks stored in the worker node
	IntermediateDataAggregated AggregatedIntermediateTokens
	Instances                  map[int]WorkerInstanceInternal //server for each worker istance Id
	ControlRpcInstance         WorkerInstanceInternal         //worker node main control instace ->chunks&&respawn
	ReducersClients            map[int]*rpc.Client
	Id                         int
	ExitChan                   chan bool
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
	//println("init new instance on worker: ", worker.Id, "with Id: ", maxId)
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
		mapper := new(MapperIstanceStateInternal)
		if initData != nil {
			mapper = &(initData.MapData)
		}
		err = workerIstanceData.Server.RegisterName("MAP", mapper)
		mapper.WorkerChunks = &workerNodeInt.WorkerChunksStore //link to chunks for mapper
		workerIstanceData.IntData.MapData = MapperIstanceStateInternal(*mapper)
	} else if kind == REDUCE {
		reducer := new(ReducerIstanceStateInternal)
		if initData != nil {
			reducer = &(initData.ReduceData)
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
	//insert the newly created instance
	if kind != CONTROL { //append only standard instances, the control own a special field
		workerNodeInt.Instances[newId] = workerIstanceData
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
	return nil, newId
}

func (workerNodeInt *Worker_node_internal) initLogicWorkerIstance(initData *GenericInternalState, kind int, id *int) *WorkerInstanceInternal {
	/*
		initialize a new logic worker instance inside worker node
		kind used to discriminate instance tipe, initData is eventual initialization data for the new instance to create
		returned the new instance ref and eventual errors
	*/
	maxId := GetMaxIdWorkerInstances(&workerNodeInt.Instances) //find new unique ID
	newId := maxId + 1
	if id != nil { //try to assign id to the new instance if not assigned already
		_, present := workerNodeInt.Instances[*id]
		if present {
			panic("WORKER INSTANCE ID COLLISION")
		}
		newId = *id
	}
	workerIstanceData := WorkerInstanceInternal{
		ControlData: WorkerIstanceControl{
			Id:       newId,
			Kind:     kind,
			IntState: IDLE,
		}}
	if kind == MAP {
		mapper := *new(MapperIstanceStateInternal)
		mapper.IntermediateTokens = make(map[string]int)
		if initData != nil {
			mapper = (initData.MapData)
		}
		mapper.WorkerChunks = &workerNodeInt.WorkerChunksStore //link to chunks for mapper
		workerIstanceData.IntData.MapData = mapper
	} else if kind == REDUCE {
		reducer := new(ReducerIstanceStateInternal)
		if initData != nil {
			reducer = &(initData.ReduceData)
		}
		workerIstanceData.IntData.ReduceData = *reducer
	} else if kind == CONTROL { //instances for controlRPCs and ChunksService at workerNodeLevel
		//chunk service
		workerNodeInt.WorkerChunksStore.Chunks = make(map[int]CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		workerNodeInt.ControlRpcInstance = workerIstanceData
	} else {
		panic("unrecognized code" + strconv.Itoa(kind))
	}
	if kind != CONTROL { //append only standard instances, the control own a special field
		workerNodeInt.Instances[newId] = workerIstanceData
	}
	return &workerIstanceData
}
