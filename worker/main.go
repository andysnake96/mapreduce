package main

import (
	"../core"
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
		workerNodeInternal.Instances = make(map[int]core.WorkerInstanceInternal)
		workerNodeInternal.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, core.WORKER_CHUNKS_INITSIZE_DFLT)
		e1, _ := core.InitRPCWorkerIstance(nil, core.Config.CHUNK_SERVICE_BASE_PORT, core.CONTROL, &workerNodeInternal)
		e2, _ := core.InitRPCWorkerIstance(nil, port, rpcCODE, &workerNodeInternal)
		core.CheckErrs([]error{e1, e2}, true, "instantiating base instances...")
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
	core.AssignedPortsAll = make([]int, 0, totalWorkerNum)
	//initalize workers by kinds specified in configuration file starting with the chunk service instance
	for i := 0; i < core.Config.WORKER_NUM_MAP; i++ {
		workerInternalNode = &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Instances = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, core.WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		e, _ := core.InitRPCWorkerIstance(nil, avaiblePort, core.CONTROL, workerInternalNode)
		core.CheckErr(e, true, "instantiating base instances...")
		//TODO heartbit monitoring service for each worker
		workerId++
	}
	for i := 0; i < core.Config.WORKER_NUM_ONLY_REDUCE; i++ {
		workerInternalNode := &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Instances = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, core.WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		e, _ := core.InitRPCWorkerIstance(nil, avaiblePort, core.CONTROL, workerInternalNode)
		core.CheckErrs([]error{e}, true, "instantiating base instances...")
		workerId++
	}
	for i := 0; i < core.Config.WORKER_NUM_BACKUP_WORKER; i++ {
		workerInternalNode := &workersNodeInternal_localVersion[workerId]
		workerInternalNode.Instances = make(map[int]core.WorkerInstanceInternal)
		workerInternalNode.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, core.WORKER_CHUNKS_INITSIZE_DFLT)
		avaiblePort = core.NextUnassignedPort(core.Config.CHUNK_SERVICE_BASE_PORT, &core.AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
		e, _ := core.InitRPCWorkerIstance(nil, avaiblePort, core.CONTROL, workerInternalNode)
		core.CheckErr(e, true, "init backup instancez")
		workerId++

	}
	println(core.AssignedPortsAll)

}
