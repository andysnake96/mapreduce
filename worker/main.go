package main

import (
	"../core"
	"math/rand"
	"os"
	"reflect"
	"time"
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
var simulateKillStartChan chan bool
var stopPingService chan bool
const(
	WORKER_CRUSH_SIMULATE_NUM = 5
)
var SIMULATE_WORKER_CRUSH_BEFORE time.Duration= 1 * time.Nanosecond

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
		stopPingService=make(chan bool,1)
		core.InitWorkers_LocalMock_WorkerSide(&WorkersNodeInternal_localVersion,stopPingService)
		if core.Config.SIMULATE_WORKERS_CRUSH{
			go simulateWorkerCrushRandomly(WorkersNodeInternal_localVersion)
		}
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

func simulateWorkerCrushRandomly(workers []core.Worker_node_internal) {
	//simulate crush of workers at random time in a range

	//////////// wait for first master connection
	chanSet := []reflect.SelectCase{}
	for _, worker := range WorkersNodeInternal_localVersion {
		chanSet = append(chanSet, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(worker.StartChan),
		})
	}
	from, chanOut, _ := reflect.Select(chanSet)						///block until first rpc of master
	//chanOut := valValue.Interface().(int)
	println("first rpc on worker: ", from, reflect.ValueOf(chanOut).Interface()," trigger worker crash simulation")
	workersIndxsDeathnote:=make(map[int]bool,WORKER_CRUSH_SIMULATE_NUM)
	workerUnluckyIndx := -1
	for i := 0; i < WORKER_CRUSH_SIMULATE_NUM; i++ {
		redundantShoot:=true
		for ; redundantShoot;  {	//while chosen a new worker to kill
			workerUnluckyIndx=rand.Intn(len(workers))
			_,redundantShoot=workersIndxsDeathnote[workerUnluckyIndx]
		}
		//kill uniquelly random chosen worker
		workersIndxsDeathnote[workerUnluckyIndx]=true	//set unlucky worker on already chosen map
		workerUnlucky:=workers[workerUnluckyIndx]
		go func(workerChosen core.Worker_node_internal){ //async schedule workers kill on new routine
			///select a random time before simulate worker death
			killAfterAbout:=rand.Intn(int(SIMULATE_WORKER_CRUSH_BEFORE))
			time.Sleep(time.Duration(killAfterAbout))
			/// on routine resume kill all instances on unlucky worker
			_ = workerChosen.ControlRpcInstance.ListenerRpc.Close()
			_=  workerChosen.PingConnection.Close()
			for _, instance := range workerChosen.Instances {
				_ = instance.ListenerRpc.Close()
				println("killed instance under ",instance.ListenerRpc.Addr().String())
			}
			println("killed worker under ",workerChosen.ControlRpcInstance.ListenerRpc.Addr().String())
			//runtime.Goexit()
		}(workerUnlucky)
	}
	//runtime.Goexit()
}

func waitWorkersEnd() {
	chanSet := []reflect.SelectCase{}
	for _, worker := range WorkersNodeInternal_localVersion {
		chanSet = append(chanSet, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(worker.ExitChan),
		})
	}
	workersToWait := len(WorkersNodeInternal_localVersion)
	if core.Config.SIMULATE_WORKERS_CRUSH {
		workersToWait -= WORKER_CRUSH_SIMULATE_NUM
	}
	for i := 0; i < workersToWait; i++ {
		//<-worker.ExitChan
		endedWorkerID, chanOut, _ := reflect.Select(chanSet)
		worker:=WorkersNodeInternal_localVersion[endedWorkerID]
		//_ = worker.PingConnection.Close()
		_ = worker.ControlRpcInstance.ListenerRpc.Close()
		//chanOut := valValue.Interface().(int)
		println("ended worker: ", endedWorkerID, reflect.ValueOf(chanOut).Interface())
	}
	stopPingService<-true											//quit all ping routine

}