package main

import (
	"../aws_SDK_wrap"
	"../core"
	"encoding/gob"
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
	master code, various configurations avaible in both configuration file and program argument
	master will wait workers to register and start map reduce on completed initialization

	Chunks for map will be sent in fair shares to worker with a replication configurable
	is possible to send only chunk IDs and worker will retrive them on S3
	at the end of map workers will return only their distribuition of bindings of intermediate data derived from map processing to reducers
	reflecting distribution of interm.tokens among workers.
	on fault of a mapper master will re assigned failed map jobs exploiting data locality of chunks replication among workers
	so some jobs may be unnecessary to be rescheduled.
	Master will exploit this to select best worker node to host reducer instances producing reducer bindings to comunicate to mappers
	mappers will route the right share (pre  aggregated with a combiner logic) of their interm data (comunicated by master by IDs)
	to reducer. In that route data locality of interm. data is exploited because reducers placement will avoid biggest data share to send
	for load balancing and fault tollerantit's possible to configure in config.json a number of reducer instances that have to be spawned on isolated node
	reducer will terminate when all of expected interm.data is received (each interm.data sent from mapper contains the source chunks IDs that has generated it)
	and at the end will sent final, aggregated tokens to master.
	master will sort on config and flush to a file.
	on fault this last phase will be evaluated both mappers and reducers to be respwned
	using both a ping aliveness filter and error code propagated (eventually) backward in master->mapper->reducer routing
*/

const INIT_FINAL_TOKEN_SIZE = 500

var MasterControl core.MASTER_CONTROL

func main() {
	//// read config file, if S3 config file flag is given overwrite local configuration with the one on S3
	core.Config = new(core.Configuration)
	core.ReadConfigFile(core.CONFIGFILEPATH, core.Config)
	if core.Config.UPDATE_CONFIGURATION_S3 { //read config file from S3 on argv flag setted
		//download config file from S3
		downloader := aws_SDK_wrap.GetS3Downloader(core.Config.S3_REGION)
		const INITIAL_CONFIG_FILE_SIZE = 2048
		buf := make([]byte, INITIAL_CONFIG_FILE_SIZE)
		err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, core.CONFIGFILENAME, &buf, false)
		core.CheckErr(err, true, "config file read from S3 error")
		core.DecodeConfigFile(strings.NewReader(string(buf)), core.Config) //decode downloaded config file
	}

	MasterControl = core.MASTER_CONTROL{}
	var masterAddress string
	var err error

	////// master working config
	if len(os.Args) < 3 {
		println("usage: isMasterCopy,publicIP master,configFile on S3 source file1,...source fileN")
		os.Exit(1)
	} //TODO ARGV TEMPLATE

	//// master address
	//masterAddress = "37.116.178.139" //dig +short myip.opendns.com @resolver1.opendns.com
	if len(os.Args) >= 3 { //TODO SWTICH TO ARGV TEMPLATE
		masterAddress = os.Args[2]

	}
	//// master replica
	if core.Config.BACKUP_MASTER {
		gob.Register(core.MapWorkerArgs{}) //register sub field of  master state
		gob.Register(core.Map2ReduceRouteCost{})
		isMasterReplicaStr := "false"
		if len(os.Args) >= 2 { //TODO SWTICH TO ARGV TEMPLATE
			isMasterReplicaStr = os.Args[1]
		}
		if strings.Contains(strings.ToUpper(isMasterReplicaStr), "TRUE") {
			MasterReplicaStart(masterAddress)
		}
	}
	//// filenames
	filenames := core.FILENAMES_LOCL
	if len(os.Args) > 4 { //TODO SWTICH TO ARGV TEMPLATE
		filenames = os.Args[4:]
	}
	MasterControl.MasterAddress = masterAddress
	startInitTime := time.Now()
	uploader, _, err := core.Init_distribuited_version(&MasterControl, filenames, core.Config.LoadChunksToS3)
	println("elapsed for initialization: ", time.Now().Sub(startInitTime).String())
	if core.CheckErr(err, false, "") {
		killAll(&MasterControl.Workers)
		os.Exit(96)
	}

	masterData := masterRpcInit()
	MasterControl.MasterRpc = masterData
	masterLogic(core.CHUNK_ASSIGN, &MasterControl, false, uploader)
}

func masterLogic(startPoint uint32, masterControl *core.MASTER_CONTROL, isReplica bool, uploader *aws_SDK_wrap.UPLOADER) {

	var startTime time.Time
	var err bool
	var errs []error
	//var errs []error
	//// from given starting point
	switch startPoint {
	case core.CHUNK_ASSIGN:
		goto chunk_assign
	case core.LOCALITY_AWARE_LINK_REDUCE:
		goto locality_aware_link_reduce
	}

	///// CHUNK ASSIGNEMENT & MAP
chunk_assign:
	if isReplica {
		isReplica = false //TODO FIND BETTER WAY TO AVOID STUPID CHECK IN NEXT LABEL
		masterControl.MasterData.MapResults, err = RecoveryMapResults(masterControl)
		goto checkMap
	}
	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	startTime = time.Now()
	masterControl.MasterData.AssignedChunkWorkers = make(map[int][]int)
	masterControl.MasterData.AssignedChunkWorkersFairShare = assignChunksIDs(masterControl.Workers.WorkersMapReduce, (masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR, false, masterControl.MasterData.AssignedChunkWorkers)
	checkMapRes(masterControl)
	assignChunksIDs(masterControl.Workers.WorkersBackup, (masterControl.MasterData.ChunkIDS), core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, true, masterControl.MasterData.AssignedChunkWorkers) //only replication assignement on backup workers
	//// checkpoint master state with assignement
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.CHUNK_ASSIGN
		masterControl.StateChan <- core.CHUNK_ASSIGN
		backUpMasterState(masterControl, uploader)
	}
	/*
		assign individual map jobs to specific workers,
		they will retun control information about distribution of their intermediate token to (logic) reducers
		With these information logic reducers will be instantiated on workers exploiting intermediate data locality to route
	*/

	masterControl.MasterData.MapResults, err = assignMapJobsWithRedundancy(&masterControl.MasterData, &masterControl.Workers, masterControl.MasterData.AssignedChunkWorkers, masterControl.MasterData.AssignedChunkWorkersFairShare) //RPC 2,3 IN SEQ DIAGRAM
	println("elapsed: ", time.Now().Sub(startTime).String())

checkMap:
	if err {
		_, _ = fmt.Fprintf(os.Stderr, "ASSIGN MAP JOBS ERRD\n RETRY ON FAILED WORKERS EXPLOITING ASSIGNED CHUNKS REPLICATION")
		reassignementResult := MapPhaseRecovery(masterControl, nil, uploader) //TODO ADD morefails
		if !reassignementResult {
			if core.Config.FAIL_RETRY > 0 {
				core.Config.FAIL_RETRY--
				println("RETRY...")
				goto checkMap
			}
			killAll(&masterControl.Workers)
			os.Exit(96)
		}
	}
	//filter away replication worker among map results
	masterControl.MasterData.MapResults = filterFoundamentalMapResult(masterControl.MasterData.MapResults, masterControl.MasterData.AssignedChunkWorkersFairShare)
	////DATA LOCALITY AWARE REDUCER COMPUTATION && map intermadiate data set
	masterControl.MasterData.ReducerRoutingInfos = aggregateMappersCosts(masterControl.MasterData.MapResults, &masterControl.Workers)
	masterControl.MasterData.ReducerSmartBindingsToWorkersID = core.ReducersBindingsLocallityAwareEuristic(masterControl.MasterData.ReducerRoutingInfos.DataRoutingCosts, &masterControl.Workers)
	//// checkpoint master state
	if core.Config.BACKUP_MASTER {
		masterControl.State = core.LOCALITY_AWARE_LINK_REDUCE
		masterControl.StateChan <- core.LOCALITY_AWARE_LINK_REDUCE
		backUpMasterState(masterControl, uploader)
	}
locality_aware_link_reduce:
	reduceCallTriggered := false
	if isReplica {
		fails := RecoveryReduceResults(masterControl)
		if !fails {
			goto finalAggreagate
		}
	}
	startTime = time.Now()
	/*
		instantiate logic reducer on actual worker and communicate  these bindings to workers with map instances
		they will aggregate reduce calls to individual reducers propagating  eventual errors
	*/
	////DATA LOCALITY AWARE REDUCER BINDINGS COMMUNICATION
	reduceCallTriggered, errs = comunicateReducersBindings(masterControl, masterControl.MasterData.ReducerSmartBindingsToWorkersID, false) //RPC 4,5 IN SEQ DIAGRAM;
	println("elapsed: ", time.Now().Sub(startTime).String())
	/*if !isReplica && core.Config.BACKUP_MASTER {
		os.Exit(69)
	}*/
checkReduce:
	if len(errs) > 0 {
		_, _ = fmt.Fprint(os.Stderr, "FAIL DURING REDUCE PHASE, reduceTriggered: ", reduceCallTriggered)
		//set up list for failed instances inside failed workers (mapper & reducer)
		moreFails := core.PingProbeAlivenessFilter(masterControl, false) //filter in place failed workers TODO ADD TO MAP PHASE RECOVERY morefails
		if len(masterControl.WorkersAll) < core.Config.MIN_WORKERS_NUM {
			_, _ = fmt.Fprint(os.Stderr, "TOO MUCH WORKER FAILS... ABORTING COMPUTATION..")
			killAll(&masterControl.Workers)
			os.Exit(96)
		}
		mapToRedo, reduceToRedo := core.ParseReduceErrString(errs, &masterControl.MasterData, moreFails)
		///MAPS REDO
		if len(mapToRedo) > 0 {
			reassignementResult := MapPhaseRecovery(masterControl, mapToRedo, uploader)
			if !reassignementResult {
				if core.Config.FAIL_RETRY > 0 {
					core.Config.FAIL_RETRY--
					println("RETRY...")
					goto checkReduce
				}
				killAll(&masterControl.Workers)
				os.Exit(96)
			}
		}
		checkMapRes(masterControl)
		///REDUCES REDO
		if len(reduceToRedo) > 0 {
			//re assign failed reducer on avaible workers following custom order for better assignements in accord with load balance
			//in place modified prev. bindings
			_ = ReducersReplacementRecovery(reduceToRedo, masterControl.MasterData.ReducerSmartBindingsToWorkersID, &masterControl.Workers)
			//re notify mappers reducer placement
		}
		if core.Config.BACKUP_MASTER {

			backUpMasterState(masterControl, uploader) //checkpoint new reducer bindings
		}
		_, errs = comunicateReducersBindings(masterControl, masterControl.MasterData.ReducerSmartBindingsToWorkersID, reduceCallTriggered)
		if len(errs) > 0 {
			_, _ = fmt.Fprintf(os.Stderr, "error on map RE reduce comunication")
			if core.Config.FAIL_RETRY > 0 {
				core.Config.FAIL_RETRY--
				println("  RETRY...")
				goto checkReduce
			}
			killAll(&masterControl.Workers)
			panic("")
			os.Exit(96)
		}
	} //checkpoint avoided here because of reduce link comunication return when mappers already called REDUCE
	err = jobsEnd(masterControl) //wait all reduces END then, kill all workers
	if err {
		goto locality_aware_link_reduce
	}
finalAggreagate:
	if core.Config.SORT_FINAL {
		tk := core.TokenSorter{masterControl.MasterRpc.FinalTokens}
		sort.Sort(sort.Reverse(tk))
	}
	core.SerializeToFile(masterControl.MasterRpc.FinalTokens, core.FINAL_TOKEN_FILENAME)
	println(masterControl.MasterRpc.FinalTokens)
	os.Exit(0)
}

func filterFoundamentalMapResult(mapResults []core.MapWorkerArgsWrap, filterWorkersResults map[int][]int) []core.MapWorkerArgsWrap {
	//among all map result filter away replication results for locality aware placement
	//filterWorkersResults equally to the individual and unique chunk share assigned to workers
	filteredMapResult := make([]core.MapWorkerArgsWrap, 0, len(mapResults))
	for _, mapRes := range mapResults {
		_, isWorkerFoundamental := filterWorkersResults[mapRes.WorkerId]
		if isWorkerFoundamental {
			filteredMapResult = append(filteredMapResult, mapRes)
		} else {
			print("TODO filtered --> ", mapRes.WorkerId)
		}
	}
	return filteredMapResult
}

func checkMapRes(control *core.MASTER_CONTROL) {

	allChunksAssigned := make([]int, 0, len(control.MasterData.ChunkIDS))
	for _, chunks := range control.MasterData.AssignedChunkWorkersFairShare {

		allChunksAssigned = append(allChunksAssigned, chunks...)
	}
	sort.Ints(allChunksAssigned)
	alreadySeen := make(map[int]bool)
	for _, value := range allChunksAssigned {
		_, BUG := alreadySeen[value]
		if BUG {
			panic("")
		}
		alreadySeen[value] = true
	}
	//if len(allChunksAssigned) != len(control.MasterData.ChunkIDS) {
	//	panic("WTF")
	//}
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
func assignChunksIDs(workers []core.Worker, chunksIds []int, replicationFactor int, onlyReplication bool, globalChunkAssignement map[int][]int) map[int][]int {
	/*
		fair share of chunks assigned to each worker plus a replication factor of chunks
		only latter if onlyReplication is true
		global assignement  handled by a global var globalChunkAssignement for replication
		fairShare of chunks needed for map assigned to a special field of worker
		return the fair share assignements to each worker passed,

	*/
	if len(workers) == 0 {
		return nil
	}
	_fairChunkNumShare := len(chunksIds) / len(workers)
	fairChunkNumShare := int(len(chunksIds) / len(workers))
	if _fairChunkNumShare < 1 {
		fairChunkNumShare = 1
	}
	assignementFairShare := make(map[int][]int, len(workers))
	chunkIndex := 0
	if !onlyReplication { //evaluate both fair assignement and replication
		for i, worker := range workers {
			if chunkIndex >= len(chunksIds) {
				break //too few chunks for workers ammount
			}
			chunkIDsFairShare := (chunksIds)[i*fairChunkNumShare : (i+1)*fairChunkNumShare]
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], chunkIDsFairShare...) //quicklink for smart replication
			assignementFairShare[worker.Id] = copySlice(chunkIDsFairShare)
			chunkIndex += fairChunkNumShare
		}

		if (len(chunksIds) - chunkIndex) > 0 { //chunks residues not assigned yet
			//println("chunk fair share remainder (-eq)", len(chunksIds)-chunkIndex, fairChunkNumRemider)
			//last worker will be in charge for last chunks
			worker := (workers)[len(workers)-1]
			lastShare := (chunksIds)[chunkIndex:]
			globalChunkAssignement[worker.Id] = append(globalChunkAssignement[worker.Id], lastShare...)
			assignementFairShare[worker.Id] = append(assignementFairShare[worker.Id], lastShare...)

		}
	}
	//CHUNKS REPLICATION
	for _, worker := range workers {
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

func copySlice(ints []int) []int {
	out := make([]int, len(ints))
	copy(out, ints)
	return out
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

func assignMapJobsWithRedundancy(data *core.MASTER_STATE_DATA, workers *core.WorkersKinds, chunkShare map[int][]int, chunkFairShare map[int][]int) ([]core.MapWorkerArgsWrap, bool) {
	/*
		trigger map operation over chunk share per worker
		chunkShare is a per worker chunks share including a configurable ammount of replication
		chunkFairShare is individual per worker chunks share with no replication (used only for the return)
		each worker will execute MAP logic over each chunk and will aggregate routing cost of chunk related to chunkFairShare
		routing cost of intermediate data to reducers reflect data locality among workers and will be exploited for smart placement of reducers
		maximizing data locality already present on destination node in next phase
	*/
	println("-----\t", "Assigning MAP jobs to worker with Redundancy levels: ", core.Config.CHUNKS_REPLICATION_FACTOR, core.Config.CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS, "\t-----")
	hasErrd := false
	mapRpcWrap := make([]core.MapWorkerArgsWrap, 0, len(chunkShare))
	/// building worker map jobs arg for rpc
	for workerId, chunkIDs := range chunkShare {
		workerMapJobs := core.MapWorkerArgsWrap{
			MapJobArgs: core.MapWorkerArgs{
				ChunkIds:          chunkIDs,
				ChunkIdsFairShare: chunkFairShare[workerId],
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
		core.GenericPrint(mapRpcWrap[i].MapJobArgs.ChunkIds, "assigning map jobs to worker "+strconv.Itoa(mapRpcWrap[i].WorkerId)+"\t"+workerPntr.Address+" : "+strconv.Itoa(workerPntr.State.ControlRPCInstance.Port))
		mapRpcWrap[i].End = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Go("CONTROL.AssignMaps", mapRpcWrap[i].MapJobArgs, &(mapRpcWrap[i].Reply), nil)
	}
	var err error
	for i := 0; i < len(mapRpcWrap); i++ {
		select {
		case <-mapRpcWrap[i].End.Done:
			err = mapRpcWrap[i].End.Error
		case <-time.After(core.TIMEOUT_PER_RPC):
			err = errors.New("RPC TIMEOUT")
		}
		if core.CheckErr(err, false, "error on worker :"+strconv.Itoa(mapRpcWrap[i].WorkerId)) {
			mapRpcWrap[i].Err = err
			hasErrd = true
			(core.GetWorker(mapRpcWrap[i].WorkerId, workers, true)).State.Failed = true
		}
	}
	return mapRpcWrap, hasErrd
}

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

func comunicateReducersBindings(control *core.MASTER_CONTROL, reducersBindings map[int]int, failPostPartialReduce bool) (bool, []error) {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings dict
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram
	println("-----\t", "Comunicating Data Locality Aware Reducers Bindings triggering Reduce calls", "\t-----")

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
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)+"\t at; \t"+worker.Address) {
			worker.State.Failed = true
			errs = append(errs, errors.New(core.REDUCER_ACTIVATE+core.ERROR_SEPARATOR+strconv.Itoa(reducerIdLogic)))
			continue //don't appending failed reducer until activation
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker, "\t", reducerBindings[reducerIdLogic])
		worker.State.ReducersHostedIDs = append(worker.State.ReducersHostedIDs, reducerIdLogic)
	}
	if len(errs) > 0 { //no reduce call has been triggered, hopefully only reducers has to respawned
		_, _ = fmt.Fprint(os.Stderr, "some reducers activation has failed")
		return false, errs
	}

	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS WORKERS

	ends := make([]*rpc.Call, 0, len(control.MasterData.AssignedChunkWorkersFairShare))
	mappersErrs := make([][]error, len(control.MasterData.AssignedChunkWorkersFairShare))
	mappersTriggerReduce := make([]int, 0, len(control.MasterData.AssignedChunkWorkersFairShare))
	i := 0

	checkMapRes(control)
	for workerID, chunksShare := range control.MasterData.AssignedChunkWorkersFairShare {
		worker := core.GetWorker(workerID, &(control.Workers), true)
		arg := core.ReduceTriggerArg{
			ReducersAddresses:     reducerBindings,
			IndividualChunkShare:  chunksShare,
			FailPostPartialReduce: failPostPartialReduce,
		}
		core.GenericPrint(chunksShare, "assigning to worker: "+strconv.Itoa(workerID)+" this chunks to aggregate and route")
		callAsync := (*rpc.Client)(worker.State.ControlRPCInstance.Client).Go("CONTROL.ReducersCollocations", arg, &(mappersErrs[i]), nil)
		ends = append(ends, callAsync)
		mappersTriggerReduce = append(mappersTriggerReduce, workerID)
		i++
	}
	//wait rpc return
	for i, end := range ends {
		select { //APP LEVEL TIMEOUT PER RPC
		case <-end.Done:
		case <-time.After(core.TIMEOUT_PER_RPC):
			end.Error = errors.New("RPC TIMEOUT") //explicit override error field on timeout
		}
		endWorkerID := mappersTriggerReduce[i]
		if core.CheckErr(end.Error, false, "error on bindings comunication at "+strconv.Itoa(endWorkerID)) {
			if mappersErrs[i] != nil {
				errs = append(errs, mappersErrs[i]...) //set propagated error from mapper
			} else { //if not set means mapper has failed
				errs = append(errs, errors.New(core.REDUCERS_ADDR_COMUNICATION+core.ERROR_SEPARATOR+strconv.Itoa(endWorkerID)))
			}
		}
	}
	return true, errs
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

var TIMEOUT = time.Second * 5

func jobsEnd(control *core.MASTER_CONTROL) bool {
	/*
		block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
		TODO if timeout expire means all workers has failed and reducer failed with result to send
	*/
	println("waiting for reducers ends")
	for r := 0; r < core.Config.ISTANCES_NUM_REDUCE; r++ {
		select {
		case <-*(*control).MasterRpc.ReturnedReducer: //wait end of all reducers
		case <-time.After(TIMEOUT):
			return true
		}
		println("ENDED REDUCER!!!", r)
	}
	killAll(&(control.Workers))
	return false
}
