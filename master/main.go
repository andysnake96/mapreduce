package main

import (
	"../aws_SDK_wrap"
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

const INIT_FINAL_TOKEN_SIZE = 500
const TIMEOUT_PER_RPC time.Duration = time.Second * 2

func main() {
	core.Config = new(core.Configuration)
	core.Addresses = new(core.WorkerAddresses)
	core.ReadConfigFile(core.CONFIGFILENAME, core.Config)
	core.ReadConfigFile(core.ADDRESSES_GEN_FILENAME, core.Addresses)
	masterControl := core.MASTER_CONTROL{}
	var masterAddress string
	var err error
	var uploader *aws_SDK_wrap.UPLOADER = nil
	if core.Config.LOCAL_VERSION {
		masterAddress = "localhost"
		init_local_version(&masterControl)
	} else {

		////// master working config
		println("usage: isMasterCopy,publicIP master, data upload to S3, source file1,...source fileN")

		//// master address
		//masterAddress = "37.116.178.139" //dig +short myip.opendns.com @resolver1.opendns.com
		masterAddress = ""     //dig +short myip.opendns.com @resolver1.opendns.com
		if len(os.Args) >= 3 { //TODO SWTICH TO ARGV TEMPLATE
			masterAddress = os.Args[2]
		}
		//// master replica
		isMasterReplicaStr := "false"
		if len(os.Args) >= 2 { //TODO SWTICH TO ARGV TEMPLATE
			isMasterReplicaStr = os.Args[1]
		}
		if strings.Contains(strings.ToUpper(isMasterReplicaStr), "TRUE") {
			MasterReplicaStart(masterAddress)
		}
		//// load chunks flag
		loadChunksToS3 := false
		loadChunksToS3Str := "false"
		if len(os.Args) >= 4 {
			loadChunksToS3Str = os.Args[3]
		}
		if strings.Contains(strings.ToUpper(loadChunksToS3Str), "TRUE") {
			loadChunksToS3 = true
		}

		//// filenames
		filenames := core.FILENAMES_LOCL
		if len(os.Args) >= 5 { //TODO SWTICH TO ARGV TEMPLATE
			filenames = os.Args[4:]
		}
		masterControl.MasterAddress = masterAddress
		startInitTime := time.Now()
		uploader, _, err = core.Init_distribuited_version(&masterControl, filenames, loadChunksToS3)
		println("elapsed for initialization: ", time.Now().Sub(startInitTime).String())
		if core.CheckErr(err, false, "") {
			killAll(&masterControl.Workers)
			os.Exit(96)
		}
	}
	masterData := masterRpcInit()
	masterControl.MasterRpc = masterData
	masterLogic(core.CHUNK_ASSIGN, &masterControl, uploader)
}

func masterLogic(startPoint uint32, masterControl *core.MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) {

	var startTime time.Time
	var err bool
	//var errs []error
	//// from given starting point
	switch startPoint {
	case core.CHUNK_ASSIGN:
		goto chunk_assign
	//case core.MAP_ASSIGN:
	//	goto map_assign
	case core.LOCALITY_AWARE_LINK_REDUCE:
		goto locality_aware_link_reduce
	}

	/////CHUNK ASSIGNEMENT
chunk_assign:
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.CHUNK_ASSIGN
		masterControl.StateChan <- core.CHUNK_ASSIGN
		err := backUpMasterState(masterControl, uploader)
		if core.CheckErr(err, false, "") {
			_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
			killAll(&masterControl.Workers)
		}
	}

	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	startTime = time.Now()
	masterControl.MasterData.AssignedChunkWorkers = make(map[int][]int)
	masterControl.MasterData.AssignedChunkWorkersFairShare = assignChunksIDs(&masterControl.Workers.WorkersMapReduce, &(masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR, false, masterControl.MasterData.AssignedChunkWorkers)
	assignChunksIDs(&masterControl.Workers.WorkersBackup, &(masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, true, masterControl.MasterData.AssignedChunkWorkers) //only replication assignement on backup workers

	/*errs = comunicateChunksAssignementToWorkers(masterControl.MasterData.AssignedChunkWorkers, &masterControl.Workers) //RPC 1 IN SEQ DIAGRAM
		println("elapsed for chunk assignement: ", time.Now().Sub(startTime).String())
		if len(errs) > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "ASSIGN CHUNK ERRD \n")
			workersIdsToReschedule := make(map[int][]int, len(errs)) //map of worker->chunks assigned to reschedule
			for _, err := range errs {
				workerIdErrd, _ := strconv.Atoi(err.Error()) //failed worker ID appended during comunication
				workerErrd := core.GetWorker(workerIdErrd, &(masterControl.Workers))
				workerErrd.State.Failed = true
				core.CheckErr(err, false, "failed worker id:"+strconv.Itoa(workerIdErrd))
				chunkIds := masterControl.MasterData.AssignedChunkWorkers[workerIdErrd]
				workersIdsToReschedule[workerIdErrd] = chunkIds
				//workerErrd,err:=core.GetWorker(workerIdErrd,&masterControl.Workers)
			}
			moreFailsIDs := core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers
			//evaluate possible extra fails not reported in chunk assign errs reuturn (failed between assignEND<->ping filter
			for id, _ := range moreFailsIDs {
				chunksAssigned := masterControl.MasterData.AssignedChunkWorkersFairShare[id]
				if chunksAssigned != nil {
					workersIdsToReschedule[id] = chunksAssigned
				}
			}
			newChunksAssignements := AssignChunksIDsRecovery(&masterControl.Workers, workersIdsToReschedule,
				&(masterControl.MasterData.AssignedChunkWorkers), &(masterControl.MasterData.AssignedChunkWorkersFairShare))
			errs = comunicateChunksAssignementToWorkers(newChunksAssignements, &masterControl.Workers) //RPC 1 IN SEQ DIAGRAM
			if len(errs) > 0 {
				_, _ = fmt.Fprintf(os.Stderr, "REASSIGNEMENT OF CHUNK ID FAILED...\n \t aborting all\n")
				killAll(&masterControl.WorkersAll)
				os.Exit(96)
			}

		}

		////	MAP
	map_assign:
		if core.Config.BACKUP_MASTER {
			masterControl.State = core.MAP_ASSIGN
			<-masterControl.StateChan //TODO MASTER BACKUP OFF TEST
			masterControl.StateChan <- core.MAP_ASSIGN
			println("backup master state...")
			err := backUpMasterState(masterControl, uploader)
			if core.CheckErr(err, false, "") {
				_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
				killAll(&masterControl.WorkersAll)
			}
		}
	*/ //TODO RPC1-2-3 MERGED
	/*
		assign individual map jobs to specific workers,
		they will retun control information about distribution of their intermediate token to (logic) reducers
		With these information logic reducers will be instantiated on workers exploiting intermediate data locality to route
	*/

	masterControl.MasterData.MapResults, err = assignMapJobsWithRedundancy(&masterControl.MasterData, &masterControl.Workers, masterControl.MasterData.AssignedChunkWorkers) //RPC 2,3 IN SEQ DIAGRAM
	println("elapsed: ", time.Now().Sub(startTime).String())

	if err {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN MAP JOBS ERRD\n RETRY ON FAILED WORKERS EXPLOITING ASSIGNED CHUNKS REPLICATION")

		reassignementResult := MapPhaseRecovery(masterControl, nil)
		if !reassignementResult {
			killAll(&masterControl.Workers)
			os.Exit(96)
		}
	}

	////DATA LOCALITY AWARE REDUCER COMPUTATION && map intermadiate data set
locality_aware_link_reduce:
	/*if core.Config.BACKUP_MASTER {
		masterControl.State = core.LOCALITY_AWARE_LINK_REDUCE
		<-masterControl.StateChan //TODO MASTER BACKUP OFF TEST
		masterControl.StateChan <- core.LOCALITY_AWARE_LINK_REDUCE
		err := backUpMasterState(masterControl, uploader)
		if core.CheckErr(err, false, "") {
			_, _ = fmt.Fprint(os.Stderr, "MASTER STATE BACKUP FAILED, ABORTING")
			killAll(&masterControl.Workers)
		}
	}*/
	startTime = time.Now()
	masterControl.MasterData.ReducerRoutingInfos = aggregateMappersCosts(masterControl.MasterData.MapResults, &masterControl.Workers)
	masterControl.MasterData.ReducerSmartBindingsToWorkersID = core.ReducersBindingsLocallityAwareEuristic(masterControl.MasterData.ReducerRoutingInfos.DataRoutingCosts, &masterControl.Workers)
	////DATA LOCALITY AWARE REDUCER BINDINGS COMMUNICATION
	/*
		instantiate logic reducer on actual worker and communicate  these bindings to workers with map instances
		they will aggregate reduce calls to individual reducers propagating  eventual errors
	*/

	reduceCallsOk, errs := comunicateReducersBindings(masterControl, masterControl.MasterData.ReducerSmartBindingsToWorkersID) //RPC 4,5 IN SEQ DIAGRAM;
	println("elapsed: ", time.Now().Sub(startTime).String())

	if len(errs) > 0 {
		//set up list for failed instances inside failed workers (mapper & reducer)
		moreFails := core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers
		mapToRedo, reduceToRedo := core.ParseReduceErrString(errs, &masterControl.MasterData, moreFails)
		///MAPS REDO
		var newMapBindings map[int][]int = nil
		if len(mapToRedo) > 0 {
			reassignementResult := MapPhaseRecovery(masterControl, mapToRedo)
			if !reassignementResult {
				killAll(&masterControl.Workers)
				os.Exit(96)
			}
		}

		///REDUCES REDO
		recoveryBindings := make(map[int]int)
		if len(reduceToRedo) > 0 {
			//re assign failed reducer on avaible workers following custom order for better assignements in accord with load balance
			recoveryBindings = ReducersReplacementRecovery(reduceToRedo, newMapBindings, &masterControl.MasterData.ReducerSmartBindingsToWorkersID, &masterControl.Workers)

			//as before comunicate newly re spawned reducer on worker -> NB mappers will trasparently re send per reducer intermediate data
			if reduceCallsOk > 0 {
				//if reduce bindings has been already comunicated but an error occurred, re activate/comunicate only newly spawned reducers
				_, errs = comunicateReducersBindings(masterControl, recoveryBindings)
			} else {
				_, errs = comunicateReducersBindings(masterControl, masterControl.MasterData.ReducerSmartBindingsToWorkersID)
			}
			if len(errs) > 0 {
				_, _ = fmt.Fprintf(os.Stderr, "error on map RE reduce comunication")
				panic("")
				//killAll(&masterControl.Workers)
				os.Exit(96)
			}
		}
	}
	// will be triggered in 6
	jobsEnd(masterControl) //wait all reduces END then, kill all workers
	if core.Config.SORT_FINAL {
		tk := core.TokenSorter{masterControl.MasterRpc.FinalTokens}
		sort.Sort(sort.Reverse(tk))
	}
	core.SerializeToFile(masterControl.MasterRpc.FinalTokens, core.FINAL_TOKEN_FILENAME)
	println(masterControl.MasterRpc.FinalTokens)
	os.Exit(0)
}

func checkMapRes(control *core.MASTER_CONTROL) {
	allChunksAssigned := make([]int, 0, len(control.MasterData.ChunkIDS))
	for _, mapRes := range control.MasterData.MapResults {
		allChunksAssigned = append(allChunksAssigned, mapRes.MapJobArgs.ChunkIds...)
	}

	sort.Ints(allChunksAssigned)
	core.GenericPrint(allChunksAssigned, "global assignement of chunks")
	allChunksAssigned = make([]int, 0, len(control.MasterData.ChunkIDS))
	for _, chunks := range control.MasterData.AssignedChunkWorkersFairShare {

		allChunksAssigned = append(allChunksAssigned, chunks...)
	}
	sort.Ints(allChunksAssigned)
	core.GenericPrint(allChunksAssigned, "global assignement of chunks")
	if len(allChunksAssigned) != len(control.MasterData.ChunkIDS) {
		panic("WTF")
	}
}

func init_local_version(control *core.MASTER_CONTROL) {
	////// init files
	var filenames []string = core.FILENAMES_LOCL
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}

	////// load chunk to storage service
	control.MasterData.ChunkIDS = core.LoadChunksStorageService_localMock(filenames)
	////// init workers
	control.Workers, control.WorkersAll = core.InitWorkers_LocalMock_MasterSide() //TODO BOTO3 SCRIPT CONCURRENT STARTUP
	//creating workers ref
}

func masterRpcInit() *core.MasterRpc {
	//register master RPC
	reducerEndChan := make(chan bool, core.Config.ISTANCES_NUM_REDUCE) //buffer all reducers return flags for later check (avoid block during rpc return )
	master := core.MasterRpc{
		FinalTokens:     make([]core.Token, 0, INIT_FINAL_TOKEN_SIZE),
		Mutex:           core.MUTEX{},
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
	if len(*workers) == 0 {
		return nil
	}
	_fairChunkNumShare := len(*chunksIds) / len(*workers)
	fairChunkNumShare := int(len(*chunksIds) / len(*workers))
	if _fairChunkNumShare < 1 {
		fairChunkNumShare = 1
	}
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
			chunkIndex += fairChunkNumShare

		}

		if (len(*chunksIds) - chunkIndex) > 0 { //chunks residues not assigned yet
			//println("chunk fair share remainder (-eq)", len(*chunksIds)-chunkIndex, fairChunkNumRemider)
			//last worker will be in charge for last chunks
			worker := (*workers)[len(*workers)-1]
			lastShare := (*chunksIds)[chunkIndex:]
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], lastShare...)
			assignementFairShare[worker.Id] = append(assignementFairShare[worker.Id], lastShare...)

		}
	}
	//CHUNKS REPLICATION
	for _, worker := range *workers {
		////reminder assignment //todo old
		//worker.State.ChunksIDs = append(worker.State.ChunksIDs, chunkIDsFairShareReminder...)                   // will append an empty list if OnlyReplciation is true//TODO CHECK DEBUG
		//globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShareReminder...) //quick link for smart replication
		////replication assignment
		chunksReplicationToAssignID, err := core.GetChunksNotAlreadyAssignedRR(chunksIds, replicationFactor, globalChunkAssignement[worker.Id])
		core.CheckErr(err, false, "chunks replication assignment impossibility, chunks saturation on workers")
		globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunksReplicationToAssignID...) //quick link for smart replication
	}
	return assignementFairShare
}

func comunicateChunksAssignementToWorkers(assignementChunkWorkers map[int][]int, workers *core.WorkersKinds) []error {
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
			workerPntr := core.GetWorker(workerId, workers, true)
			time.Sleep(time.Second * 2)
			ends[i] = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.Get_chunk_ids", chunksIds, nil, nil)
			workersAssigned[i] = workerPntr
			i++
		}
	}
	var divCall *rpc.Call
	//timeout:=TIMEOUT_PER_RPC
	//startTime:=time.Now()
	for i, doneChan := range ends { //wait all assignment compleated
		hasTimedOut := false
		if doneChan != nil {
			select {
			case divCall = <-doneChan.Done:
				//case <-time.After(TIMEOUT_PER_RPC):
				//	{
				//		_, _ = fmt.Fprintf(os.Stderr, "RPC TIMEOUT\n")
				//		hasTimedOut = true
				//	}
			}
			worker := *(workersAssigned[i])
			if hasTimedOut || core.CheckErr(divCall.Error, false, "chunkAssign Res on"+strconv.Itoa(worker.Id)) {
				//errors=append(errors,divCall.Error)
				errs = append(errs, errors.New(strconv.Itoa(worker.Id))) //append worker id of failed rpc
				continue
			}
			worker.State.ChunksIDs = append(worker.State.ChunksIDs, assignementChunkWorkers[worker.Id]...) //eventually append correctly assigned chunk to worker
		}
	}
	return errs
}

func assignMapJobsWithRedundancy(data *core.MASTER_STATE_DATA, workers *core.WorkersKinds, chunkShare map[int][]int) ([]core.MapWorkerArgsWrap, bool) {
	/*
		assign MAP input data (chunks) to designed workers that will trigger several concurrent MAP execution
		intermediate tokens data buffered inside workers and routing cost of data to reducers (logic) returned aggregated at worker node level
		that reflect data locality of interm.data over workers,
		if newChunks is not nil it will be used to filter appended chunk data to map request to workers (
	*/
	hasErrd := false
	mapRpcWrap := make([]core.MapWorkerArgsWrap, 0, len(chunkShare))
	/// building worker map jobs arg for rpc
	for workerId, chunkIDs := range chunkShare {
		workerMapJobs := core.MapWorkerArgsWrap{
			MapJobArgs: core.MapWorkerArgs{
				ChunkIds: chunkIDs,
			},
			WorkerId: workerId,
			Err:      nil,
			Reply:    core.Map2ReduceRouteCost{},
		}
		if !core.Config.LoadChunksToS3 {
			workerMapJobs.MapJobArgs.Chunks = make([]core.CHUNK, len(chunkIDs))
			//// append chunk data only if not already cached in dest worker
			for i, chunkId := range chunkIDs {
				workerMapJobs.MapJobArgs.Chunks[i] = data.Chunks[chunkId]
			}

		}
		mapRpcWrap = append(mapRpcWrap, workerMapJobs)
	}

	////////////// MAP RPC CALLS
	for i := 0; i < len(mapRpcWrap); i++ {
		workerPntr := core.GetWorker(mapRpcWrap[i].WorkerId, workers, true)
		//async start map
		core.GenericPrint(mapRpcWrap[i].MapJobArgs.ChunkIds, "assigning map jobs to worker"+strconv.Itoa(mapRpcWrap[i].WorkerId)+"\t"+strconv.Itoa(workerPntr.State.ControlRPCInstance.Port))
		mapRpcWrap[i].End = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.AssignMaps", mapRpcWrap[i].MapJobArgs, &(mapRpcWrap[i].Reply), nil)
	}
	for i := 0; i < len(mapRpcWrap); i++ {
		<-mapRpcWrap[i].End.Done
		err := mapRpcWrap[i].End.Error
		if core.CheckErr(err, false, "error on workerMap:"+strconv.Itoa(mapRpcWrap[i].WorkerId)) {
			mapRpcWrap[i].Err = err
			hasErrd = true
			(core.GetWorker(mapRpcWrap[i].WorkerId, workers, true)).State.Failed = true

		}
	}
	return mapRpcWrap, hasErrd
}

//func assignMapWorks(workerMapperChunks map[int][]int, workers *core.WorkersKinds) ([]core.MapWorkerArgs, bool) {
//	/*
//		assign MAP input data (chunks) to designed workers that will trigger several concurrent MAP execution
//		intermediate tokens data buffered inside workers and routing cost of data to reducers (logic) returned aggregated at worker node level
//		that reflect data locality of interm.data over workers,
//	*/
//
//	hasErrd := false
//	mapRpcWrap := make([]core.MapWorkerArgs, len(workerMapperChunks))
//	i := 0
//	for workerId, chunkIDs := range workerMapperChunks {
//		workerPntr := core.GetWorker(workerId, workers)
//		mapRpcWrap[i] = core.MapWorkerArgs{
//			ChunkIds: chunkIDs,
//			WorkerId: workerId,
//			Err:      nil,
//			Reply:    core.Map2ReduceRouteCost{},
//		}
//		//async start map
//		mapRpcWrap[i].End = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.DoMAPs", mapRpcWrap[i].ChunkIds, &(mapRpcWrap[i].Reply), nil)
//		i++
//	}
//	for _, mapArg := range mapRpcWrap {
//		<-mapArg.End.Done
//		err := mapArg.End.Error
//		if core.CheckErr(err, false, "error on workerMap:"+strconv.Itoa(mapArg.WorkerId)) {
//			mapArg.Err = err
//			hasErrd = true
//		}
//	}
//	return mapRpcWrap, hasErrd
//}

func aggregateMappersCosts(workerMapResults []core.MapWorkerArgsWrap, workers *core.WorkersKinds) core.ReducersRouteInfos {
	//for each mapper worker aggregate route infos

	//nested dict for route infos
	workersMapRouteCosts := make(map[int]map[int]int, len(workerMapResults))
	workersMapExpectedReduceCalls := make(map[int]map[int]int, len(workerMapResults))
	for _, workerResult := range workerMapResults {
		//init aggreagate infos nested dicts
		workerId := workerResult.WorkerId
		worker := core.GetWorker(workerId, workers, true)
		worker.State.MapIntermediateTokenIDs = append(worker.State.MapIntermediateTokenIDs, workerResult.MapJobArgs.ChunkIds...) //set intermadiate data inside worker
		workersMapRouteCosts[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		workersMapExpectedReduceCalls[workerId] = make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		//aggreagate infos
		for reducer, routeCostTo := range workerResult.Reply.RouteCosts {
			workersMapRouteCosts[workerId][reducer] = routeCostTo
		}
		for reducer, expectedCallsToFromMapper := range workerResult.Reply.RouteNum {
			workersMapExpectedReduceCalls[workerId][reducer] = expectedCallsToFromMapper
		}
	}
	routeInfosAggregated := core.ReducersRouteInfos{
		DataRoutingCosts:           core.ReducersDataRouteCosts{workersMapRouteCosts},
		ExpectedReduceCallsMappers: workersMapExpectedReduceCalls,
	}
	return routeInfosAggregated
}

func comunicateReducersBindings(control *core.MASTER_CONTROL, reducersBindings map[int]int) (int, []error) {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings dict
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram

	redNewInstancePort := 0
	errs := make([]error, 0)

	/// RPC 4 ---> REDUCERS INSTANCES ACTIVATION
	reducerBindings := make(map[int]string, core.Config.ISTANCES_NUM_REDUCE)
	for reducerIdLogic, placementWorker := range reducersBindings {
		worker := core.GetWorker(placementWorker, &control.Workers, true) //get dest worker for the new Reduce Instance
		//instantiate the new reducer instance with expected calls # from mapper for termination and faultTollerant
		arg := core.ReduceActiveArg{
			NumChunks: len(control.MasterData.ChunkIDS),
			LogicID:   reducerIdLogic,
		}
		err := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Call("CONTROL.ActivateNewReducer", arg, &redNewInstancePort)
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)) {
			worker.State.Failed = true
			errs = append(errs, errors.New(core.REDUCER_ACTIVATE+core.ERROR_SEPARATOR+strconv.Itoa(reducerIdLogic)))
			continue //don't appending failed reducer until activation
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker, "\t", reducerBindings[reducerIdLogic])
		worker.State.ReducersHostedIDs = append(worker.State.ReducersHostedIDs, reducerIdLogic)
	}
	if len(errs) > 0 {
		_, _ = fmt.Fprint(os.Stderr, "some reducers activation has failed")
		return 0, errs
	}
	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS WORKERS
	//comunicate to all mappers final Reducer location
	correctReduceCalls := 0
	ends := make([]*rpc.Call, 0, len(control.MasterData.MapResults))
	mappersErrs := make([][]error, len(control.MasterData.MapResults))
	i := 0

	checkMapRes(control)
	for workerID, chunksShare := range control.MasterData.AssignedChunkWorkersFairShare {
		worker := core.GetWorker(workerID, &(control.Workers), true)
		arg := core.ReduceTriggerArg{
			ReducersAddresses: reducerBindings,
			ChunksToAggregate: chunksShare,
		}
		callAsync := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Go("CONTROL.ReducersCollocations", arg, &(mappersErrs[i]), nil)
		ends = append(ends, callAsync)
		correctReduceCalls++
		i++
	}
	//wait rpc return
	for i, end := range ends {
		<-end.Done
		if core.CheckErr(end.Error, false, "error on bindings comunication") {
			failedWorkerID := control.MasterData.MapResults[i].WorkerId
			errs = append(errs, errors.New(core.REDUCERS_ADDR_COMUNICATION+core.ERROR_SEPARATOR+strconv.Itoa(failedWorkerID)))
			correctReduceCalls--
		}
	}
	return correctReduceCalls, errs
}

func killAll(workersKinds *core.WorkersKinds) {
	//now reducers has returned, workers can end safely
	workers := append(workersKinds.WorkersMapReduce, workersKinds.WorkersOnlyReduce...)
	workers = append(workersKinds.WorkersBackup, workers...)
	for _, worker := range workers {
		println("Exiting worker: ", worker.Id)
		err := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Call("CONTROL.ExitWorker", 0, nil)
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
	killAll(&(control.Workers))
}
