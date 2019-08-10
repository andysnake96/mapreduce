package main

import (
	"../core"
	"errors"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

/*

	//TODO FAULT TOLLERANT MAIN LINES
		-REDUCER FAULT=> reducer re instantiation (least loaded) mappers comunication of new reducer (re send intermediate data only to him)
		-WORKER MAPPER FAULT=> MAP jobs reassign considering chunks replication distribuition and REDUCER INTERMDIATE DATA AGGREGATED COSTRAINT	(SEE TODO IN REDUCER INT STATA)
							(e.g. multiple mapper worker fail after some REDUCE() => different expected REDUCE from reducers )
							WISE MAP JOBs REASSIGN AND SCHEDULE ....options:
								-master ask from reducers expected intermdiate data, in map reassign comunicated that---> mapper will wisely aggreagate chunks to avoid collisions
								-not aggregated map result to route to reducers (avoid possibility intermd.data conflict on reducer)



TODO...
		@) al posto di heartbit monitoring continuo-> prendo errori da risultati di RPC e ne valuto successivamente soluzioni
			->eventuale heartbit check per vedere se worker è morto o solo istanza
*/
//// MASTER CONTROL VARS
var Workers core.WorkersKinds //connected workers
var WorkersAll []*core.Worker //list of all workers ref.
var TotalWorkersNum int
var ChunkIDS []int
var AssignedChunkWorkers map[int][]int //workerID->assigned Cunks
var reducerEndChan chan bool

const INIT_FINAL_TOKEN_SIZE = 500

func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)
	//TODO NOTES ON DISTRIBUITED VERSION
	////// init files
	/*
		passed filenames (metadata (filename->size) present on master disk)
	*/
	/// load chunk to storage service
	/*
			alternative:
				-chunkization & load to storage service S3 (boto3 scritp) here
				-auto script chunkization & load (split & boto3) prima di chiamare main master....

		->check che tutti i chunk sono presenti
	*/
	////// init workers
	/*
		boto 3 script worker init.... filled worker structs with addresses
	*/
	if core.Config.LOCAL_VERSION {
		init_local_version()
	} /*else{

	}*/
	masterRpcInit()
	/////	assign chunks
	///init vars ....
	AssignedChunkWorkers = make(map[int][]int)
	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	workersChunksFairShares := assignChunksIDs(&Workers.WorkersMapReduce, &ChunkIDS, core.Config.CHUNKS_REPLICATION_FACTOR, false)
	println(workersChunksFairShares)
	assignChunksIDs(&Workers.WorkersBackup, &ChunkIDS, core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, true) //only replication assignement on backup workers
	errs := comunicateChunksAssignementToWorkers()                                                                 //RPC 1 IN SEQ DIAGRAM
	if errs != nil {
		println(errs) //TODO FAULT TOLLERANT LOGIC on FAILED WORKER ID
	}
	////	MAP
	mapResults, err := assignMapWorks(workersChunksFairShares) //RPC 2,3 IN SEQ DIAGRAM
	if err {
		//TODO trigger map instances reassign on replysErrors (per instance error infos)
	}
	////	DATA LOCALITY AWARE REDUCER COMPUTATION
	reducerRoutingInfos := aggregateMappersCosts(mapResults)
	reducerSmartBindingsToWorkersID := core.ReducersBindingsLocallityAwareEuristic(reducerRoutingInfos.DataRoutingCosts, &Workers)
	println(reducerSmartBindingsToWorkersID, reducerSmartBindingsToWorkersID)
	////	DATA LOCALITY AWARE REDUCER BINDINGS COMUNICATION AND REDUCE TRIGGER
	rpcErrs := comunicateReducersBindings(reducerSmartBindingsToWorkersID, reducerRoutingInfos.ExpectedReduceCallsMappers) //RPC 4,5 IN SEQ DIAGRAM;
	if rpcErrs != nil {
		println(rpcErrs) //TODO FAULT TOLLERANT LOGIC on failed rpc
	}
	// will be triggered in 6
	jobsEnd() //wait all reduces END then, kill all workers
	os.Exit(0)

}
func init_local_version() {
	////// init files
	var filenames []string = core.FILENAMES_LOCL
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}

	////// load chunk to storage service
	ChunkIDS = core.LoadChunksStorageService_localMock(filenames)
	////// init workers
	TotalWorkersNum = core.Config.WORKER_NUM_MAP + core.Config.WORKER_NUM_ONLY_REDUCE + core.Config.WORKER_NUM_BACKUP_WORKER
	Workers, WorkersAll = core.InitWorkers_LocalMock_MasterSide() //TODO BOTO3 SCRIPT CONCURRENT STARTUP
	//creating workers ref
}

func masterRpcInit() {
	//register master RPC
	reducerEndChan = make(chan bool, core.Config.ISTANCES_NUM_REDUCE) //buffer all reducers return flags for later check (avoid block during rpc return )
	master := core.Master{
		FinalTokens:     make([]core.Token, 0, INIT_FINAL_TOKEN_SIZE),
		Mutex:           sync.Mutex{},
		ReturnedReducer: &reducerEndChan,
	}
	server := rpc.NewServer()
	err := server.RegisterName("MASTER", &master)
	core.CheckErr(err, true, "master rpc register errorr")
	l, e := net.Listen("tcp", ":"+strconv.Itoa(core.Config.MASTER_BASE_PORT))
	core.CheckErr(e, true, "socket listen error")
	//go PingHeartBitRcv()
	go server.Accept(l)
}
func assignChunksIDs(workers *[]core.Worker, chunksIds *[]int, replicationFactor int, onlyReplication bool) map[int][]int {
	/*
		fair share of chunks assigned to each worker plus a replication factor of chunks
		only latter if onlyReplication is true
		global assignement  handled by a global var AssignedChunkWorkers for replication
		fairShare of chunks needed for map assigned to a special field of worker
		return the fair share assignements to each worker passed,

	*/
	_fairChunkNumShare := len(*chunksIds) / len(*workers)
	fairChunkNumShare := int(len(*chunksIds) / len(*workers))
	if _fairChunkNumShare < 1 {
		println("to few chunks or too much workers")
		fairChunkNumShare = 1
	}
	fairChunkNumRemider := int(len(*chunksIds) % len(*workers))
	assignementFairShare := make(map[int][]int, len(*workers))
	chunkIndex := 0
	if !onlyReplication { //evaluate both fair assignement and replication
		for i, worker := range *workers {
			if chunkIndex >= len(*chunksIds) {
				break //too few chunks for workers ammount
			}
			chunkIDsFairShare := (*chunksIds)[i*fairChunkNumShare : (i+1)*fairChunkNumShare]
			AssignedChunkWorkers[worker.Id] = append(AssignedChunkWorkers[worker.Id], chunkIDsFairShare...) //quicklink for smart replication
			assignementFairShare[worker.Id] = chunkIDsFairShare
			workerChunks := &(worker.State.ChunksIDs)
			*workerChunks = append((*workerChunks), chunkIDsFairShare...)
			//workerChunksFairShare := &(worker.State.ChunksIDsFairShare)
			//*workerChunksFairShare = append((*workerChunksFairShare), chunkIDsFairShare...)
			chunkIndex += fairChunkNumShare

		}

		if (len(*chunksIds) - chunkIndex) > 0 { //chunks residues not assigned yet
			//println("chunk fair share remainder (-eq)", len(*chunksIds)-chunkIndex, fairChunkNumRemider)
			//last worker will be in charge for last chunks
			worker := (*workers)[len(*workers)-1]
			lastShare := (*chunksIds)[chunkIndex:]
			AssignedChunkWorkers[worker.Id] = append(AssignedChunkWorkers[worker.Id], lastShare...)
			assignementFairShare[worker.Id] = append(assignementFairShare[worker.Id], lastShare...)
			workerChunks := &(worker.State.ChunksIDs)
			*workerChunks = append((*workerChunks), lastShare...)
			//workerChunksFairShare := &(worker.State.ChunksIDsFairShare)
			//*workerChunksFairShare = append((*workerChunksFairShare), lastShare...)
		}
	}
	//CHUNKS REPLICATION
	for _, worker := range *workers {
		////reminder assignment //todo old
		//worker.State.ChunksIDs = append(worker.State.ChunksIDs, chunkIDsFairShareReminder...)                   // will append an empty list if OnlyReplciation is true//TODO CHECK DEBUG
		//AssignedChunkWorkers[worker.Id] = append(AssignedChunkWorkers[worker.Id], chunkIDsFairShareReminder...) //quick link for smart replication
		////replication assignment
		chunksReplicationToAssignID, err := core.GetChunksNotAlreadyAssignedRR(chunksIds, replicationFactor-fairChunkNumRemider, AssignedChunkWorkers[worker.Id])
		core.CheckErr(err, false, "chunks replication assignment impossibility, chunks saturation on workers")
		AssignedChunkWorkers[worker.Id] = append(AssignedChunkWorkers[worker.Id], chunksReplicationToAssignID...) //quick link for smart replication
		worker.State.ChunksIDs = append(worker.State.ChunksIDs, chunksReplicationToAssignID...)
	}
	return assignementFairShare
}
func comunicateChunksAssignementToWorkers() []error {
	/*
		comunicate to all workers chunks assignements
		propagate eventual errors
	*/
	ends := make([]*rpc.Call, len(AssignedChunkWorkers))
	i := 0
	errs := make([]error, 0)
	//ii := 0
	workerIDs := make([]int, len(AssignedChunkWorkers))
	for workerId, chunksIds := range AssignedChunkWorkers {
		if len(chunksIds) > 0 {
			workerPntr, err := core.GetWorker(workerId, &Workers)
			core.CheckErr(err, true, "chunk assignement comunication to not founded worker")
			ends[i] = (workerPntr).State.ControlRPCInstance.Client.Go("CONTROL.Get_chunk_ids", chunksIds, &i, nil)
			workerIDs[i] = workerId
			i++

		}
	}
	for i, doneChan := range ends { //wait all assignment compleated
		if doneChan != nil {
			divCall := <-doneChan.Done
			if core.CheckErr(divCall.Error, false, "chunkAssign Res") {
				//errors=append(errors,divCall.Error)
				errs = append(errs, errors.New(strconv.Itoa(workerIDs[i]))) //append worker id of failed rpc
			}
		}
	}
	return errs
}

type MapWorkerArgs struct {
	chunkIds []int
	workerId int
	reply    core.Map2ReduceRouteCost
	end      *rpc.Call
	err      error
}

func assignMapWorks(workerMapperChunks map[int][]int) ([]MapWorkerArgs, bool) {
	/*
		assign MAP input data (chunks) to designed workers that will trigger several concurrent MAP execution
		intermediate tokens data buffered inside workers and routing cost of data to reducers (logic) returned aggregated at worker node level
		that reflect data locality of interm.data over workers,
	*/

	hasErrd := false
	mapRpcWrap := make([]MapWorkerArgs, len(workerMapperChunks))
	i := 0
	for workerId, chunkIDs := range workerMapperChunks {
		workerPntr, err := core.GetWorker(workerId, &Workers)
		core.CheckErr(err, true, "not founded mapper worker")
		mapRpcWrap[i] = MapWorkerArgs{
			chunkIds: chunkIDs,
			workerId: workerId,
			reply:    core.Map2ReduceRouteCost{},
		}
		//async start map
		mapRpcWrap[i].end = workerPntr.State.ControlRPCInstance.Client.Go("CONTROL.DoMAPs", mapRpcWrap[i].chunkIds, &(mapRpcWrap[i].reply), nil)
		//set up refs to newly created MAP instances (1 for each chunk)
		for _, chunkId := range chunkIDs {
			workerPntr.State.WorkerIstances[chunkId] = core.WorkerIstanceControl{
				Id:       chunkId,
				Kind:     core.MAP,
				IntState: core.MAP_EXEC,
			}
		}
		i++
	}
	for _, mapArg := range mapRpcWrap {
		<-mapArg.end.Done
		err := mapArg.end.Error
		if core.CheckErr(err, false, "error on workerMap:"+strconv.Itoa(mapArg.workerId)) {
			mapArg.err = err
			hasErrd = true
		}
	}
	return mapRpcWrap, hasErrd
}

func aggregateMappersCosts(workerMapResults []MapWorkerArgs) core.ReducersRouteInfos {
	//for each mapper worker aggregate route infos

	//nested dict for route infos
	workersMapRouteCosts := make(map[int]map[int]int, len(workerMapResults))
	workersMapExpectedReduceCalls := make(map[int]map[int]int, len(workerMapResults))
	for _, workerResult := range workerMapResults {
		//init aggreagate infos nested dicts
		workerId := workerResult.workerId
		workersMapRouteCosts[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		workersMapExpectedReduceCalls[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		//aggreagate infos
		for reducer, routeCostTo := range workerResult.reply.RouteCosts {
			workersMapRouteCosts[workerId][reducer] = routeCostTo
		}
		for reducer, expectedCallsToFromMapper := range workerResult.reply.RouteNum {
			workersMapExpectedReduceCalls[workerId][reducer] = expectedCallsToFromMapper
		}
	}
	routeInfosAggregated := core.ReducersRouteInfos{
		DataRoutingCosts:           core.ReducersDataRouteCosts{workersMapRouteCosts},
		ExpectedReduceCallsMappers: workersMapExpectedReduceCalls,
	}
	return routeInfosAggregated
}

func comunicateReducersBindings(redBindings map[int]int, redExpectedCalls map[int]map[int]int) []error {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings dict
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram

	redNewInstancePort := 0
	errs := make([]error, 0)
	/// RPC 4 ---> REDUCERS INSTANCES ACTIVATION
	reducerBindings := make(map[int]string, core.Config.ISTANCES_NUM_REDUCE)
	for reducerIdLogic, placementWorker := range redBindings {
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker)
		worker, err := core.GetWorker(placementWorker, &Workers) //get dest worker for the new Reduce Instance
		core.CheckErr(err, true, "reducerBindings in comunication")
		//instantiate the new reducer instance with expected calls # from mapper for termination and faultTollerant
		err = (worker).State.ControlRPCInstance.Client.Call("CONTROL.ActivateNewReducer", len(ChunkIDS), &redNewInstancePort)
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)) {
			errs = append(errs, errors.New(core.REDUCER_ACTIVATE+core.ERROR_SEPARATOR+strconv.Itoa(reducerIdLogic)))
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
		newReducerInstanceId := core.GetMaxIdWorkerInstancesGenericDict((worker.State.WorkerIstances))
		worker.State.WorkerIstances[newReducerInstanceId] = core.WorkerIstanceControl{ //init newly activate instance ref
			Id:   newReducerInstanceId,
			Port: redNewInstancePort,
			Kind: core.REDUCE,
		}
	}
	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS WORKERS
	//comunicate to all mappers final Reducer location
	ends := make([]*rpc.Call, len(Workers.WorkersMapReduce))
	mappersErrs := make([][]error, len(Workers.WorkersMapReduce))
	//endsRpc:=make([]*rpc.Call,0,len(Workers.WorkersMapReduce)*len(Workers.WorkersMapReduce[0].State.WorkerIstances))
	for i, worker := range Workers.WorkersMapReduce {
		ends[i] = worker.State.ControlRPCInstance.Client.Go("CONTROL.ReducersCollocations", reducerBindings, &(mappersErrs[i]), nil)
	}
	//wait rpc return
	for i, end := range ends {
		<-end.Done
		if core.CheckErr(end.Error, true, "error on bindings comunication") {
			errs = append(errs, errors.New(core.REDUCERS_ADDR_COMUNICATION+core.ERROR_SEPARATOR+strconv.Itoa(Workers.WorkersMapReduce[i].Id)))
		}
	}
	return errs
}

func jobsEnd() {
	/*
		block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
	*/
	for r := 0; r < core.Config.ISTANCES_NUM_REDUCE; r++ {
		<-reducerEndChan //wait end of all reducers
		println("ENDED REDUCER!!!", r)
	}
	//now reducers has returned, workers can end safely
	for _, workerPntr := range WorkersAll {
		println("Exiting worker: ", workerPntr.Id)
		err := workerPntr.State.ControlRPCInstance.Client.Call("CONTROL.ExitWorker", 0, nil)
		core.CheckErr(err, true, "error in shutdowning worker: "+strconv.Itoa(workerPntr.Id))
	}
}
