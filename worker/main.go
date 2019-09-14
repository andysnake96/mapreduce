package main

import (
	"../aws_SDK_wrap"
	"../core"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
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
var SIMULATE_WORKER_CRUSH_BEFORE time.Duration = 500 * time.Millisecond
var SIMULATE_WORKER_CRUSH_AFTER time.Duration = 100 * time.Millisecond

func main() {
	core.Config = new(core.Configuration)
	core.ReadConfigFile(core.CONFIGFILEPATH, core.Config)
	stopPingService := make(chan bool, 1)

	/*if core.Config.LOCAL_VERSION {
		ChunkIDS := core.LoadChunksStorageService_localMock(core.FILENAMES_LOCL)
		println(ChunkIDS, "<--- chunkIDS ")
		core.InitWorkers_LocalMock_WorkerSide(&WorkersNodeInternal_localVersion, stopPingService)
		syscall.Pause()
	}*/ //TODO REMOVE
	//////distribuited  version
	///s3 links
	println("usage for non default setting: configFileFromS3, Schedule Random Crush")
	downloader, _ := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)
	if core.Config.UPDATE_CONFIGURATION_S3 { //read config file from S3 on argv flag setted
		//download config file from S3
		const INITIAL_CONFIG_FILE_SIZE = 4096
		buf := make([]byte, INITIAL_CONFIG_FILE_SIZE)
		err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, core.CONFIGFILENAME, &buf, false)
		core.CheckErr(err, true, "config file read from S3 error")
		core.DecodeConfigFile(strings.NewReader(string(buf)), core.Config) //decode downloaded config file
	}
	assignedPorts, err := core.InitWorker(&WorkersNodeInternal, stopPingService, downloader)
	core.GenericPrint(assignedPorts, "")
	core.CheckErr(err, true, "worker init error")
	portToComunicate := ""
	if !core.Config.FIXED_PORT {
		portToComunicate = strconv.Itoa(WorkersNodeInternal.ControlRpcInstance.Port) + core.PORT_SEPARATOR + strconv.Itoa(WorkersNodeInternal.PingPort) + core.PORT_TERMINATOR
	}
	masterAddr, err := core.RegisterToMaster(downloader, portToComunicate)
	core.CheckErr(err, true, "master register err")
	WorkersNodeInternal.MasterAddr = masterAddr

	//fault simulation
	isUnluckyWorker := false //worker to crush
	if len(os.Args) < 3 {
		//no crush flag given, gen with random probability in respect with workers to crush
		totalNumWorkers := core.Config.WORKER_NUM_ONLY_REDUCE + core.Config.WORKER_NUM_BACKUP_WORKER + core.Config.WORKER_NUM_MAP
		crushProbability := float64(core.Config.SIMULATE_WORKERS_CRUSH_NUM) / float64(totalNumWorkers)
		isUnluckyWorker = core.RandomBool(crushProbability, 4)
	} else {
		crushArg := strings.ToUpper(os.Args[2])
		isUnluckyWorker = strings.Contains(crushArg, "TRUE")
	}

	if core.Config.SIMULATE_WORKERS_CRUSH && isUnluckyWorker {
		simulateWorkerCrush()
	}
	waitWorkerEnd(stopPingService)
	os.Exit(0)
}

const EXIT_FORCED = 556

func simulateWorkerCrush() {
	///select a random time before simulate worker death
	killAfterAbout := rand.Int63n(int64(SIMULATE_WORKER_CRUSH_BEFORE))
	killAfterAbout += int64(SIMULATE_WORKER_CRUSH_AFTER)
	if killAfterAbout > 1 {
		time.Sleep(time.Duration(killAfterAbout))
	}
	///close every listening socket still open
	//_ = WorkersNodeInternal.ControlRpcInstance.ListenerRpc.Close()
	//for _, instance := range WorkersNodeInternal.Instances {
	//	_ = instance.ListenerRpc.Close()
	//}
	_, _ = fmt.Fprint(os.Stderr, "CRUSH SIMULATION ON WORKER with pid:", os.Getpid())
	os.Exit(EXIT_FORCED)
}

func waitWorkerEnd(stopPing chan bool) { //// distribuited version
	<-WorkersNodeInternal.ExitChan ///wait worker end setted by the master
	_ = WorkersNodeInternal.ControlRpcInstance.ListenerRpc.Close()
	//for _, instance := range WorkersNodeInternal.Instances {
	//	_ = instance.ListenerRpc.Close()
	//}
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

	for i := 0; i < workersToWait; i++ {
		//<-worker.ExitChan
		endedWorkerID, chanOut, _ := reflect.Select(chanSet)
		//worker := WorkersNodeInternal_localVersion[endedWorkerID]
		//_ = worker.ControlRpcInstance.ListenerRpc.Close()
		//for _, instance := range worker.Instances {
		//	_ = instance.ListenerRpc.Close()
		//}
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
