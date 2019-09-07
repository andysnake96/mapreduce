package main

import (
	"../aws_SDK_wrap"
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
var WorkersNodeInternal core.Worker_node_internal                //worker nod ref distribuited version

const (
	WORKER_CRUSH_SIMULATE_NUM = 3
)

var SIMULATE_WORKER_CRUSH_BEFORE time.Duration = 1 * time.Nanosecond

func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)
	stopPingService := make(chan bool, 1)

	if core.Config.LOCAL_VERSION {
		ChunkIDS := core.LoadChunksStorageService_localMock(core.FILENAMES_LOCL)
		println(ChunkIDS, "<--- chunkIDS ")
		core.InitWorkers_LocalMock_WorkerSide(&WorkersNodeInternal_localVersion, stopPingService)
	} else { //////distribuited  version
		///s3 links
		downloader, _ := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)
		assignedPorts, err := core.InitWorker(&WorkersNodeInternal, stopPingService)
		core.GenericPrint(assignedPorts)
		core.CheckErr(err, true, "worker init error")
		_, err = core.RegisterToMaster(downloader)
		core.CheckErr(err, true, "master register err")
		if core.Config.SIMULATE_WORKERS_CRUSH {
			simulateWorkerCrush()
		}
		waitWorkerEnd(stopPingService)
	}
	os.Exit(0)
}

const EXIT_FORCED = 556

func simulateWorkerCrush() {
	///select a random time before simulate worker death
	killAfterAbout := rand.Intn(int(SIMULATE_WORKER_CRUSH_BEFORE))
	time.Sleep(time.Duration(killAfterAbout))
	///close every listening socket still open
	_ = WorkersNodeInternal.ControlRpcInstance.ListenerRpc.Close()
	for _, instance := range WorkersNodeInternal.Instances {
		_ = instance.ListenerRpc.Close()
	}
	os.Exit(EXIT_FORCED)
}

func waitWorkerEnd(stopPing chan bool) { //// distribuited version
	<-WorkersNodeInternal.ExitChan ///wait worker end setted by the master
	_ = WorkersNodeInternal.ControlRpcInstance.ListenerRpc.Close()
	for _, instance := range WorkersNodeInternal.Instances {
		_ = instance.ListenerRpc.Close()
	}
	//chanOut := valValue.Interface().(int)
	println("ended worker")
	//_ = worker.PingConnection.Close()
	stopPing <- true
	time.Sleep(1)
}
func waitWorkersEnd(stopPing chan bool) { ////local version
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
		worker := WorkersNodeInternal_localVersion[endedWorkerID]
		_ = worker.ControlRpcInstance.ListenerRpc.Close()
		for _, instance := range worker.Instances {
			_ = instance.ListenerRpc.Close()
		}
		//chanOut := valValue.Interface().(int)
		println("ended worker: ", endedWorkerID, reflect.ValueOf(chanOut).Interface())
	}
	//_ = worker.PingConnection.Close()
	stopPing <- true //quit all ping routine

}

/*	//TODO OLD LOCAL VERSION OF WORKER CRUSH SIMULATE
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
*/
