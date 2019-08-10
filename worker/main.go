package main

import (
	"../core"
	"os"
	"reflect"
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

var WorkersNodeInternal_localVersion []core.Worker_node_internal //for each local simulated worker indexed by his own id -> intenal state

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
		core.InitWorkers_LocalMock_WorkerSide(&WorkersNodeInternal_localVersion)
		waitWorkersEnd()
		os.Exit(0)
	} else {
		//errs := make([]error, 3)
		//var err error
		//port, err := strconv.Atoi(os.Args[1])
		//errs = append(errs, err)
		//rpcCODE, err := strconv.Atoi(os.Args[2])
		//errs = append(errs, err)
		//usage := "<distribuited PORT,WORKER INSTANCE KIND>"
		//core.CheckErrs(errs, true, usage)
		////init node structs...
		//workerNodeInternal.Instances = make(map[int]core.WorkerInstanceInternal)
		//workerNodeInternal.WorkerChunksStore.Chunks = make(map[int]core.CHUNK, core.WORKER_CHUNKS_INITSIZE_DFLT)
		//e1, _ := core.InitRPCWorkerIstance(nil, core.Config.CHUNK_SERVICE_BASE_PORT, core.CONTROL, &workerNodeInternal)
		//e2, _ := core.InitRPCWorkerIstance(nil, port, rpcCODE, &workerNodeInternal)
		//core.CheckErrs([]error{e1, e2}, true, "instantiating base instances...")
	}
}

func waitWorkersEnd() {
	chanSet := []reflect.SelectCase{}
	for _, worker := range WorkersNodeInternal_localVersion {
		chanSet = append(chanSet, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(worker.ExitChan),
		})
	}
	for i := 0; i < len(WorkersNodeInternal_localVersion); i++ {
		//<-worker.ExitChan
		from, chanOut, _ := reflect.Select(chanSet)
		//chanOut := valValue.Interface().(int)
		println("ended worker: ", from, reflect.ValueOf(chanOut).Interface())
	}

}
