package main

import (
	"../core"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

/*

	//TODO FAULT TOLLERANT MAIN LINES
		-REDUCER FAULT=> reducer re instantiation (least loaded) mappers comunication of new reducer (re send intermediate data only to him)
		-WORKER MAPPER FAULT=> MAP jobs reassign considering chunks replication distribuition and REDUCER INTERMDIATE DATA AGGREGATED COSTRAINT	(SEE TODO IN REDUCER INT STATA)
							(e.g. multiple mapper worker fail after some REDUCE() => different expected REDUCE from reducers )
							WISE MAP JOBs REASSIGN AND SCHEDULE ....options:
								-todo not aggregated map result to route to reducers (avoid possibility intermd.data conflict on reducer)

TODO...
		@) al posto di heartbit monitoring continuo-> prendo errori da risultati di RPC e ne valuto successivamente soluzioni
			->eventuale heartbit check per vedere se worker è morto o solo istanza
*/

var MasterControl core.MASTER_CONTROL

const INIT_FINAL_TOKEN_SIZE = 500
const TIMEOUT_PER_RPC time.Duration = time.Second
const FINAL_TOKEN_FILENAME = "outTokens.list"

func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)

	var masterAddress string
	masterRpcPort := core.Config.MASTER_BASE_PORT

	///distribuited  version vars
	//var downloader *s3manager.Downloader
	//var uploader *s3manager.Uploader
	//var assignedPort []int
	if core.Config.LOCAL_VERSION {
		masterAddress = "localhost"
		init_local_version(&MasterControl)
	} else {
		println("usage: publicIP master, upload to S3, source file1,...source fileN")
		filenames := core.FILENAMES_LOCL //TODO SWITCH TO ARGV
		//filenames:=os.Args[3:]

		//masterAddress=os.Args[1]
		masterAddress = "37.116.178.139" //dig +short myip.opendns.com @resolver1.opendns.com

		loadChunksToS3 := false
		loadChunksToS3Str := "true"
		//loadChunksToS3Str:=os.Args[2]
		if strings.Contains(strings.ToUpper(loadChunksToS3Str), "TRUE") {
			loadChunksToS3 = true
		}
		MasterControl.Addresses.Master = masterAddress
		_, _, _ = core.Init_distribuited_version(&MasterControl, filenames, loadChunksToS3)
	}
	masterData := masterRpcInit()
	MasterControl.MasterRpc = masterData
	Workers := MasterControl.Workers
	WorkersAll := MasterControl.WorkersAll
	/////CHUNK ASSIGNEMENT
	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	assignedChunkWorkers := make(map[int][]int)
	workersChunksFairShares := assignChunksIDs(&Workers.WorkersMapReduce, &(MasterControl.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR, false, assignedChunkWorkers)
	//core.GenericPrint(workersChunksFairShares)
	assignChunksIDs(&Workers.WorkersBackup, &(MasterControl.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, true, assignedChunkWorkers) //only replication assignement on backup workers
	time.Sleep(time.Second)
	errs := comunicateChunksAssignementToWorkers(assignedChunkWorkers) //RPC 1 IN SEQ DIAGRAM
	if len(errs) > 0 {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN CHUNK ERRD \n")
		workersIdsToReschedule := make(map[int][]int, len(errs)) //map of worker->chunks assigned to reschedule
		for _, err := range errs {
			workerIdErrd, _ := strconv.Atoi(err.Error()) //failed worker ID appended during comunication
			core.CheckErr(err, false, "failed worker id:"+strconv.Itoa(workerIdErrd))
			chunkIds := assignedChunkWorkers[workerIdErrd]
			workersIdsToReschedule[workerIdErrd] = chunkIds
			//workerErrd,err:=core.GetWorker(workerIdErrd,&Workers)
		}
		core.PingProbeAlivenessFilter(&WorkersAll) //filter in place failed workers
		newChunksAssignements := AssignChunksIDsRecovery(&Workers, workersIdsToReschedule, &assignedChunkWorkers)
		errs := comunicateChunksAssignementToWorkers(newChunksAssignements)
		if len(errs) > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "REASSIGNEMENT OF CHUNK ID FAILED...\n \t aborting all\n")
			killAll(&WorkersAll)
			os.Exit(96)
		}
	}

	////	MAP
	/*
		assign individual map jobs to specific workers,
		they will retun control information about distribution of their intermediate token to (logic) reducers
		With these information logic reducers will be instantiated on workers exploiting intermediate data locality to route
	*/
	mapResults, err := assignMapWorks(workersChunksFairShares, &Workers) //RPC 2,3 IN SEQ DIAGRAM
	if err {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN MAP JOBS ERRD\n RETRY ON FAILED WORKERS EXPLOITING ASSIGNED CHUNKS REPLICATION")
		workerMapJobsToReassign := make(map[int][]int) //workerId--> map job to redo (chunkID previusly assigned)
		for indx, mapResult := range mapResults {
			if core.CheckErr(mapResult.err, false, "WORKER id:"+strconv.Itoa(mapResult.workerId)+" ON MAPS JOB ASSIGN") {
				core.PingProbeAlivenessFilter(&WorkersAll) //filter in place failed workers
				workerMapJobsToReassign[mapResult.workerId] = mapResult.chunkIds
				/// remove failed element from map
				if indx < len(mapResults)-1 {
					mapResults = append(mapResults[:indx], mapResults[indx+1:]...) //remove failed element from results
				} else {
					mapResults = mapResults[:indx] //remove only last element from slice
				}
			}
		}
		//re assign failed map job exploiting chunk replication among workers
		newMapBindings := AssignMapWorksRecovery(workerMapJobsToReassign, &Workers, &assignedChunkWorkers)
		/// retry map
		mapResultsNew, err := assignMapWorks(newMapBindings, &Workers)
		if err {
			_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign\n aborting all")
			killAll(&WorkersAll)
			os.Exit(96)
		}
		mapResults = mergeMapResults(mapResultsNew, mapResults)
	}

	////DATA LOCALITY AWARE REDUCER COMPUTATION && map intermadiate data set
	reducerRoutingInfos := aggregateMappersCosts(mapResults, &Workers)
	reducerSmartBindingsToWorkersID := core.ReducersBindingsLocallityAwareEuristic(reducerRoutingInfos.DataRoutingCosts, &Workers)
	println(reducerSmartBindingsToWorkersID, reducerSmartBindingsToWorkersID)

	////DATA LOCALITY AWARE REDUCER BINDINGS COMMUNICATION
	/*
		instantiate logic reducer on actual worker and communicate  these bindings to workers with map instances
		they will aggregate reduce calls to individual reducers propagating  eventual errors
	*/

	masterRpcAddress := masterAddress + ":" + strconv.Itoa(masterRpcPort)
	rpcErrs := comunicateReducersBindings(reducerSmartBindingsToWorkersID, &Workers, masterRpcAddress) //RPC 4,5 IN SEQ DIAGRAM;
	if len(errs) > 0 {
		//set up list for failed instances inside failed workers (mapper & reducer)
		core.PingProbeAlivenessFilter(&WorkersAll) //filter in place failed workers
		mapToRedo, reduceToRedo := core.ParseReduceErrString(rpcErrs, reducerSmartBindingsToWorkersID, &Workers)
		///MAPS REDO
		var newMapBindings map[int][]int = nil
		if len(mapToRedo) > 0 {
			newMapBindings = AssignMapWorksRecovery(mapToRedo, &Workers, &assignedChunkWorkers) //re bind maps work to workers
			mapResultsNew, err := assignMapWorks(newMapBindings, &Workers)                      //re do maps
			_ = aggregateMappersCosts(mapResultsNew, &Workers)                                  //TODO USELESS IF NOT MULTIPLE RETRIED THIS PHASE
			core.GenericPrint(mapResultsNew)
			//mapResults=mergeMapResults(mapResultsNew,mapResults)
			if err {
				_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign\n aborting ...")
				killAll(&WorkersAll)
				os.Exit(96)
			}
		}

		///REDUCE RESTART on failed reducers
		if len(reduceToRedo) > 0 {
			//re assign failed reducer on avaible workers following custom order for better assignements in accord with load balance
			newReducersBindings := ReducersReplacementRecovery(reduceToRedo, newMapBindings, &reducerSmartBindingsToWorkersID, &Workers)
			core.GenericPrint(newReducersBindings)
		}
		//as before comunicate newly re spawned reducer on worker -> NB mappers will trasparently re send per reducer intermediate data
		errs := comunicateReducersBindings(reducerSmartBindingsToWorkersID, &Workers, masterRpcAddress)
		if errs != nil {
			_, _ = fmt.Fprintf(os.Stderr, "error on map RE reduce comunication")
			killAll(&WorkersAll)
			os.Exit(96)
		}
	}
	// will be triggered in 6
	jobsEnd(&MasterControl) //wait all reduces END then, kill all workers
	if core.Config.SORT_FINAL {
		tk := core.TokenSorter{MasterControl.MasterRpc.FinalTokens}
		sort.Sort(sort.Reverse(tk))
	}
	core.SerializeToFile(MasterControl.MasterRpc.FinalTokens, FINAL_TOKEN_FILENAME)
	println(masterData.FinalTokens)
	os.Exit(0)

}

func init_local_version(control *core.MASTER_CONTROL) {
	////// init files
	var filenames []string = core.FILENAMES_LOCL
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}

	////// load chunk to storage service
	control.ChunkIDS = core.LoadChunksStorageService_localMock(filenames)
	////// init workers
	control.Workers, control.WorkersAll = core.InitWorkers_LocalMock_MasterSide() //TODO BOTO3 SCRIPT CONCURRENT STARTUP
	//creating workers ref
}

func masterRpcInit() *core.MasterRpc {
	//register master RPC
	reducerEndChan := make(chan bool, core.Config.ISTANCES_NUM_REDUCE) //buffer all reducers return flags for later check (avoid block during rpc return )
	master := core.MasterRpc{
		FinalTokens:     make([]core.Token, 0, INIT_FINAL_TOKEN_SIZE),
		Mutex:           sync.Mutex{},
		ReturnedReducer: &reducerEndChan,
	}
	server := rpc.NewServer()
	err := server.RegisterName("MASTER", &master)
	core.CheckErr(err, true, "master rpc register errorr")
	l, e := net.Listen("tcp", ":"+strconv.Itoa(core.Config.MASTER_BASE_PORT))
	core.CheckErr(e, true, "socket listen error")
	go server.Accept(l)
	return &master
}
func assignChunksIDs(workers *[]core.Worker, chunksIds *[]int, replicationFactor int, onlyReplication bool, globalChunkAssignement map[int][]int) map[int][]int {
	/*
		fair share of chunks assigned to each worker plus a replication factor of chunks
		only latter if onlyReplication is true
		global assignement  handled by a global var globalChunkAssignement for replication
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
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShare...) //quicklink for smart replication
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
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], lastShare...)
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
		//globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShareReminder...) //quick link for smart replication
		////replication assignment
		chunksReplicationToAssignID, err := core.GetChunksNotAlreadyAssignedRR(chunksIds, replicationFactor-fairChunkNumRemider, globalChunkAssignement[worker.Id])
		core.CheckErr(err, false, "chunks replication assignment impossibility, chunks saturation on workers")
		globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunksReplicationToAssignID...) //quick link for smart replication
		worker.State.ChunksIDs = append(worker.State.ChunksIDs, chunksReplicationToAssignID...)
	}
	return assignementFairShare
}

func comunicateChunksAssignementToWorkers(assignementChunkWorkers map[int][]int) []error {
	/*
		comunicate to all workers chunks assignements
		propagate eventual errors containing stringified failed workerID
	*/
	ends := make([]*rpc.Call, len(assignementChunkWorkers))
	i := 0
	errs := make([]error, 0)
	//ii := 0
	workersAssigned := make([]*core.Worker, len(assignementChunkWorkers))
	for workerId, chunksIds := range assignementChunkWorkers {
		if len(chunksIds) > 0 {
			workerPntr, err := core.GetWorker(workerId, &(MasterControl.Workers))
			core.CheckErr(err, true, "chunk assignement comunication to not founded worker")
			time.Sleep(time.Second * 2)
			ends[i] = (workerPntr).State.ControlRPCInstance.Client.Go("CONTROL.Get_chunk_ids", chunksIds, nil, nil)
			workersAssigned[i] = &workerPntr
			i++
		}
	}
	var divCall *rpc.Call
	for i, doneChan := range ends { //wait all assignment compleated
		hasTimedOut := false
		if doneChan != nil {
			select {
			case divCall = <-doneChan.Done:
				println("OK")
			case <-time.After(TIMEOUT_PER_RPC):
				{
					_, _ = fmt.Fprintf(os.Stderr, "RPC TIMEOUT\n")
					hasTimedOut = true
				}
			}
			worker := *(workersAssigned[i])
			if hasTimedOut || core.CheckErr(divCall.Error, false, "chunkAssign Res") {
				//errors=append(errors,divCall.Error)
				errs = append(errs, errors.New(strconv.Itoa(worker.Id))) //append worker id of failed rpc
				continue
			}
			worker.State.ChunksIDs = append(worker.State.ChunksIDs, assignementChunkWorkers[worker.Id]...) //eventually append correctly assigned chunk to worker
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

func assignMapWorks(workerMapperChunks map[int][]int, workers *core.WorkersKinds) ([]MapWorkerArgs, bool) {
	/*
		assign MAP input data (chunks) to designed workers that will trigger several concurrent MAP execution
		intermediate tokens data buffered inside workers and routing cost of data to reducers (logic) returned aggregated at worker node level
		that reflect data locality of interm.data over workers,
	*/

	hasErrd := false
	mapRpcWrap := make([]MapWorkerArgs, len(workerMapperChunks))
	i := 0
	for workerId, chunkIDs := range workerMapperChunks {
		workerPntr, err := core.GetWorker(workerId, workers)
		core.CheckErr(err, true, "not founded mapper worker")
		mapRpcWrap[i] = MapWorkerArgs{
			chunkIds: chunkIDs,
			workerId: workerId,
			err:      nil,
			reply:    core.Map2ReduceRouteCost{},
		}
		//async start map
		mapRpcWrap[i].end = workerPntr.State.ControlRPCInstance.Client.Go("CONTROL.DoMAPs", mapRpcWrap[i].chunkIds, &(mapRpcWrap[i].reply), nil)
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

func aggregateMappersCosts(workerMapResults []MapWorkerArgs, workers *core.WorkersKinds) core.ReducersRouteInfos {
	//for each mapper worker aggregate route infos

	//nested dict for route infos
	workersMapRouteCosts := make(map[int]map[int]int, len(workerMapResults))
	workersMapExpectedReduceCalls := make(map[int]map[int]int, len(workerMapResults))
	for _, workerResult := range workerMapResults {
		//init aggreagate infos nested dicts
		workerId := workerResult.workerId
		worker, err := core.GetWorker(workerId, workers)
		core.CheckErr(err, true, "aggregating map results")
		worker.State.MapIntermediateTokenIDs = append(worker.State.MapIntermediateTokenIDs, workerResult.chunkIds...) //set intermadiate data inside worker
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

func comunicateReducersBindings(redBindings map[int]int, workers *core.WorkersKinds, masterRpcAddress string) []error {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings dict
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram

	redNewInstancePort := 0
	errs := make([]error, 0)

	/// RPC 4 ---> REDUCERS INSTANCES ACTIVATION
	reducerBindings := make(map[int]string, core.Config.ISTANCES_NUM_REDUCE)
	for reducerIdLogic, placementWorker := range redBindings {
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker)
		worker, err := core.GetWorker(placementWorker, workers) //get dest worker for the new Reduce Instance
		core.CheckErr(err, true, "reducerBindings in comunication")
		//instantiate the new reducer instance with expected calls # from mapper for termination and faultTollerant

		newReducerArg := core.ReducerActivateArgs{
			NumChunks:     len(MasterControl.ChunkIDS),
			MasterAddress: masterRpcAddress,
		}
		err = (worker).State.ControlRPCInstance.Client.Call("CONTROL.ActivateNewReducer", newReducerArg, &redNewInstancePort)
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)) {
			errs = append(errs, errors.New(core.REDUCER_ACTIVATE+core.ERROR_SEPARATOR+strconv.Itoa(reducerIdLogic)))
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
		worker.State.ReducersHostedIDs = append(worker.State.ReducersHostedIDs, reducerIdLogic)
	}

	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS WORKERS
	//comunicate to all mappers final Reducer location
	ends := make([]*rpc.Call, len(workers.WorkersMapReduce))
	mappersErrs := make([][]error, len(workers.WorkersMapReduce))
	//endsRpc:=make([]*rpc.Call,0,len(workers.WorkersMapReduce)*len(workers.WorkersMapReduce[0].State.WorkerIstances))
	for i, worker := range workers.WorkersMapReduce {
		ends[i] = worker.State.ControlRPCInstance.Client.Go("CONTROL.ReducersCollocations", reducerBindings, &(mappersErrs[i]), nil)
	}
	//wait rpc return
	for i, end := range ends {
		<-end.Done
		if core.CheckErr(end.Error, true, "error on bindings comunication") {
			errs = append(errs, errors.New(core.REDUCERS_ADDR_COMUNICATION+core.ERROR_SEPARATOR+strconv.Itoa(workers.WorkersMapReduce[i].Id)))
		}
	}
	return errs
}

func killAll(workers *[]core.Worker) {
	//now reducers has returned, workers can end safely
	for _, worker := range *workers {
		println("Exiting worker: ", worker.Id)
		err := worker.State.ControlRPCInstance.Client.Call("CONTROL.ExitWorker", 0, nil)
		core.CheckErr(err, false, "error in shutdowning worker: "+strconv.Itoa(worker.Id))
	}
}
func jobsEnd(control *core.MASTER_CONTROL) {
	/*
		block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
	*/
	for r := 0; r < core.Config.ISTANCES_NUM_REDUCE; r++ {
		<-*(*control).MasterRpc.ReturnedReducer //wait end of all reducers
		println("ENDED REDUCER!!!", r)
	}
	killAll(&(control.WorkersAll))
}
