package main

import (
	"../core"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"syscall"
)

/*
worker initialization for master comunication
every worker node will have at least chunkService instance and var other instances
depending to the worker kind and runtime needs(e.g. fault,placement for locality aware routing)
instances running on a worker will have an progressive ID starting to first instance of ChunkService
2 version:
==>In distribuited version each worker node will have his own addresses
	and varius worker instances on different ports at worker level

==>In local version each worker is on localhost so worker instances port will be remapped globally
	workers num decided by addresses.json fields
*/

/// worker internal state
//TODO LOCAL  VERSION
var workersNodeInternal_localVersion []core.Worker_node_internal //for each local simulated worker indexed by his own id -> intenal state
var workerNodeInternal core.Worker_node_internal                 //worker internal state
const WORKER_CHUNKS_INITSIZE_DFLT = 3

func InitRPCWorkerIstance(istanceId int, port int, kind int, workerNodeInt *core.Worker_node_internal) {
	//Create an instance of structs which implements map and reduce interfaces
	//init code used to discriminate witch rpc to activate (see costants)
	var err error
	server := rpc.NewServer()
	workerIstanceData := core.WorkerInstanceInternal{
		Server:      server,
		ControlData: core.WorkerIstanceControl{IntState: core.IDLE, Port: port},
	}
	if kind == core.MAP {
		map_ := new(core.Mapper)
		err = server.RegisterName("MAP", map_)
		workerIstanceData.IntData.MapData = core.MapperIstanceStateInternal(*map_)
	} else if kind == core.REDUCE {
		reduce_ := new(core.Reducer)
		err = server.RegisterName("REDUCE", reduce_)
		workerIstanceData.IntData.ReduceData = core.ReducerIstanceStateInternal(*reduce_)
	} else if kind == core.CHUNK_ID_INIT {
		workersChunks := new(core.WorkerChunks)
		workersChunks.Chunks = make(map[int]core.CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		workerNodeInt.WorkerChunksStore = core.WorkerChunks(*workersChunks)
		err = server.RegisterName("CHUNK", workersChunks)
	} else {
		panic("unrecognized code" + strconv.Itoa(kind))
	}
	core.CheckErr(err, true, "Format of service selected  is not correct: ")
	workerIstanceData.Server = server
	workerIstanceData.ControlData = core.WorkerIstanceControl{
		Port:     port,
		Kind:     kind,
		IntState: core.IDLE,
	}
	workerNodeInt.Server[istanceId] = workerIstanceData
	// Listen for incoming tcp packets on port by specified offset of port base.
	l, e := net.Listen("tcp", ":"+strconv.Itoa(port))
	core.CheckErr(e, true, "listen err on port "+strconv.Itoa(port))
	go core.PingHeartBitRcv()
	go server.Accept(l) //TODO 4EVER BLOCK-> EXIT CONDITION DEFINE see under
	//_:=l.Close()           	//TODO will unblock rpc requests handler routine
	//runtime.Goexit()    		//routine end here
}
func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)

	if core.Config.LOCAL_VERSION {
		usage := "local version: <>"
		println(usage)
		ChunkIDS := core.LoadChunksStorageService_localMock(core.FILENAMES_LOCL)
		println(ChunkIDS, "<--- chunkIDS ")
		workersLocalInit(os.Args[1:])
		syscall.Pause()
	} else {
		errs := make([]error, 3)
		var err error
		port, err := strconv.Atoi(os.Args[1])
		errs = append(errs, err)
		rpcCODE, err := strconv.Atoi(os.Args[2])
		errs = append(errs, err)
		usage := "<distribuited PORT,WORKER INSTANCE KIND>"
		core.CheckErrs(errs, true, usage)
		//init node structs...
		workerNodeInternal.Server = make(map[int]core.WorkerInstanceInternal)
		workerNodeInternal.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		InitRPCWorkerIstance(0, core.Config.CHUNK_SERVICE_BASE_PORT, core.CHUNK_ID_INIT, &workerNodeInternal)
		InitRPCWorkerIstance(1, port, rpcCODE, &workerNodeInternal)

	}
}

func workersLocalInit(args []string) {
	//local worker init version
	//worker initialized on localhost as routine with instances running on them (logically as other routine) with unique assigned ports
	var avaiblePort int
	var workerInternalNode *core.Worker_node_internal
	totalWorkerNum := core.Config.WORKER_NUM_MAP + core.Config.WORKER_NUM_BACKUP_WORKER + core.Config.WORKER_NUM_ONLY_REDUCE
	workersNodeInternal_localVersion = make([]core.Worker_node_internal, totalWorkerNum)
	workerId := 0
	core.AssignedPortsAll = make([]int, 1)
	//initalize workers by kinds specified in configuration file starting with the chunk service instance
	//TODO FOR EACH WORKER ADD 1) HEARTBIT MONITORING SERVICE; 2)CONTROL RPC (ALSO DEF)
	for i := 0; i < core.Config.WORKER_NUM_MAP; i++ {

		workerInternalNode = &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Server = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		InitRPCWorkerIstance(0, avaiblePort, core.CHUNK_ID_INIT, workerInternalNode)
		avaiblePort = core.NextUnassignedPort(core.Config.MAP_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		InitRPCWorkerIstance(1, avaiblePort, core.MAP, workerInternalNode)
		//TODO 1) ADD ROUTINE FOR HEARTBIT MONITRING WORKER NODE SIMULATED workerId
		workerId++
	}
	for i := 0; i < core.Config.WORKER_NUM_ONLY_REDUCE; i++ {
		workerInternalNode := &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Server = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		InitRPCWorkerIstance(0, avaiblePort, core.CHUNK_ID_INIT, workerInternalNode)
		avaiblePort = core.NextUnassignedPort(core.Config.REDUCE_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		InitRPCWorkerIstance(1, avaiblePort, core.REDUCE, workerInternalNode)
		workerId++

	}
	for i := 0; i < core.Config.WORKER_NUM_BACKUP_WORKER; i++ {
		workerInternalNode := &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Server = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		InitRPCWorkerIstance(0, avaiblePort, core.CHUNK_ID_INIT, workerInternalNode)
		workerId++

	}
	println(core.AssignedPortsAll)

}
