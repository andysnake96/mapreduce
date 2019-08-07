package main

import (
	"../core"
	"errors"
	"log"
	"net"
	"net/rpc"
	"strconv"
)

/*
	TODO...
		@) al posto di heartbit monitoring continuo-> prendo errori da risultati di RPC e ne valuto successivamente soluzioni
			->eventuale heartbit check per vedere se worker è morto o solo istanza

*/
//// MASTER CONTROL VARS
var Workers core.WorkersKinds //connected workers
var ChunkIDS []int
var AssignedChunkWorkers map[int][]int //workerID->assigned Cunks
var reducerEndChan chan bool

const ( //errors kinds
	REDUCER_ACTIVATE             = "REDUCER_ACTIVATE"             //id reducer (logic)
	MAPPER_COMUNICATION_REDUCERS = "MAPPER_COMUNICATION_REDUCERS" //worker id mapperid

)

type InstanceRefRpcErrs struct {
	WorkerId   int
	InstanceId int
	call       *rpc.Call
	Error      error
	ErrorKind  string
}

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
	if core.CheckErr(err, false, "lastMapFailed") {
		//TODO trigger map instances reassign on replysErrors (per instance error infos)
	}
	reducerRoutingInfos := aggregateMappersCostsWorkerLev(mapResults)
	reducerSmartBindingsToWorkersID := core.ReducersBindingsLocallityAwareEuristic(reducerRoutingInfos.DataRoutingCosts, &Workers)
	println(reducerSmartBindingsToWorkersID, reducerSmartBindingsToWorkersID)
	////	DATA LOCALITY AWARE REDUCER BINDINGS
	rpcErrs := comunicateReducersBindings(reducerSmartBindingsToWorkersID, reducerRoutingInfos.ExpectedReduceCallsMappers) //RPC 4,5 IN SEQ DIAGRAM;
	if rpcErrs != nil {
		println(rpcErrs) //TODO FAULT TOLLERANT LOGIC on failed rpc
	}
	////	REDUCE																		// will be triggered in 6
	waitJobsEnd()           //wait reduce END
	cleanUpResidueWorkers() //close mappers not closed yet for fault tollerant

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
	Workers = core.InitWorkers_localMock() //TODO BOTO3 SCRIPT CONCURRENT STARTUP
}
func masterRpcInit() {
	//register master RPC
	master := new(core.Master)
	master.ReturnedReducer = reducerEndChan
	server := rpc.NewServer()
	err := server.RegisterName("MASTER", master)
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
			println("chunk fair share remainder (-eq)", len(*chunksIds)-chunkIndex, fairChunkNumRemider)
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

type MapWorkerRPCwrap struct { //worker Instances RPC controlVars
	Instances  []*core.WorkerIstanceControl
	divCalls   []*rpc.Call
	chunksIn   []int                            // worker->mapper Chunks input
	replysTmp  []core.Map2ReduceRouteCost       // mapperInstance -> reduceRouting infos
	Replys     map[int]core.Map2ReduceRouteCost // mapperInstance -> reduceRouting infos
	ReplysErrs []int                            // worker-> failed map replyes by instanceID
}

func assignMapWorks(workerMapperChunks map[int][]int) (map[int]MapWorkerRPCwrap, error) {

	/*
		assign chunks to MAP workerInstances on worker using workerMapperChunks
		new mapper Instances will be instanciated on designed workers
		return errors and bindings costs mapper-->reducers
							(not aggregated per worker (for cheaper map reassign on mapper instance fault))


	*/
	mapPort := 0                                                              //new mapper port
	mappersRpcWrap := make(map[int]MapWorkerRPCwrap, len(workerMapperChunks)) //rpcs quickRefs
	var err error = nil
	for _, worker := range Workers.WorkersMapReduce { //init map Instances on workers

		chunksIds := workerMapperChunks[worker.Id]
		workerMapInstances := make([]*core.WorkerIstanceControl, 0, len(chunksIds))
		for i := 0; i < len(chunksIds); i++ {
			println("init map instance on worker: ", worker.Id, " map instance num: ", i)
			err := (worker).State.ControlRPCInstance.Client.Call("CONTROL.RemoteControl_NewInstance", core.MAP, &mapPort)
			core.CheckErr(err, true, "init map Instances fail on worker: "+strconv.Itoa(worker.Id))
			newInstance, err := core.InitWorkerInstancesRef(&worker, mapPort, core.MAP)
			core.CheckErr(err, true, "init map Instances fail on worker: "+strconv.Itoa(worker.Id))
			workerMapInstances = append(workerMapInstances, &newInstance)
		}
		//rpc args Quick refs
		mappersRpcWrap[worker.Id] = MapWorkerRPCwrap{
			Instances:  workerMapInstances,
			divCalls:   make([]*rpc.Call, len(workerMapInstances)),
			replysTmp:  make([]core.Map2ReduceRouteCost, len(workerMapInstances)),
			Replys:     make(map[int]core.Map2ReduceRouteCost, len(workerMapInstances)),
			ReplysErrs: make([]int, 0, len(workerMapInstances)),
			chunksIn:   workerMapperChunks[worker.Id],
		}
	} //all map workers has been initiated on workers and connections opened

	/// MAP ASYNC CALLS (|| EXE)
	for workerId, rpcArgs := range mappersRpcWrap { //worker level
		println("maps async calls on worker: ", workerId)
		for i := 0; i < len(rpcArgs.chunksIn); i++ { //map instance levele
			rpcArgs.divCalls[i] = rpcArgs.Instances[i].Client.
				Go("MAP.Map_parse_builtin_quick_route", rpcArgs.chunksIn[i], &(rpcArgs.replysTmp[i]), nil) //MAP ASYNC
			//NB divCalls--chunks--replyTmp has the same inxeing at instance level
		}
	}
	/// MAP COLLECT RESULT
	for id, rpcArgs := range mappersRpcWrap { //worker level
		//quitting rpc calls
		for i := 0; i < len(rpcArgs.chunksIn); i++ { //mapper instance level
			divCall := rpcArgs.divCalls[i]
			<-divCall.Done
			if core.CheckErr(divCall.Error, false, "map failed at idWorker:instanceN"+strconv.Itoa(id)+strconv.Itoa(i)) {
				err = divCall.Error
				rpcArgs.ReplysErrs = append(rpcArgs.ReplysErrs, rpcArgs.Instances[i].Id) //append id of failed instance
			}
			//set reply in dict indexed by chunkID witch has generated it
			rpcArgs.Replys[rpcArgs.chunksIn[i]] = rpcArgs.replysTmp[i] //exploited common indexing between chunks<->reply
		}
	}
	return mappersRpcWrap, err
}

func aggregateMappersCostsWorkerLev(workerMapResults map[int]MapWorkerRPCwrap) core.ReducersRouteInfos {
	//for each worker aggregate map instances results
	reducersCosts := core.ReducersDataRouteCosts{TrafficCostsReducersDict: make(map[int]map[int]int, core.Config.WORKER_NUM_MAP)}
	reducersExpectedCallsFromMappers := make(map[int]map[int]int, core.Config.ISTANCES_NUM_REDUCE)
	//init 	num calls expected per reducer maps
	for i := 0; i < core.Config.ISTANCES_NUM_REDUCE; i++ {
		reducersExpectedCallsFromMappers[i] = make(map[int]int) //TODO 1)totalNumChunks in config.runtime_field 2)initial map size at 1]
	}

	for idWorker, rpcsWrap := range workerMapResults { //worker lev
		workerDataRouteCost := make(map[int]int, core.Config.ISTANCES_NUM_REDUCE)
		for chunkID, replys := range rpcsWrap.Replys {
			for reducerId, dataAmountToRoute := range replys.RouteCosts {
				println("aggregating map instances result of worker: ", idWorker, "mapper in charge for chunk: ", chunkID)
				workerDataRouteCost[reducerId] += dataAmountToRoute
			}
			for reducerId, expectedCallsAmmount := range replys.RouteNum {
				reducersExpectedCallsFromMappers[reducerId][chunkID] = expectedCallsAmmount
			}

		}
		reducersCosts.TrafficCostsReducersDict[idWorker] = workerDataRouteCost
	}
	return core.ReducersRouteInfos{
		DataRoutingCosts:           reducersCosts,
		ExpectedReduceCallsMappers: reducersExpectedCallsFromMappers,
	}
}

func comunicateReducersBindings(redBindings map[int]int, redExpectedCalls map[int]map[int]int) []InstanceRefRpcErrs {
	//for each reducer ID (logic) activate an actual reducer on a worker following redBindings
	// init each reducer with expected reduce calls from mappers indified by their assigned chunk (that has produced map result -> reduce calls)
	//RPC 4,5 in SEQ diagram
	redNewInstancePort := 0
	voidReply := 0
	endsRpc := make([]InstanceRefRpcErrs, 0, len(Workers.WorkersMapReduce)*len(Workers.WorkersMapReduce[0].State.WorkerIstances))
	errsAtLeastOne := false
	/// RPC 4 ---> REDUCERS INSTANCES ACTIVATION
	reducerBindings := make(map[int]string, core.Config.ISTANCES_NUM_REDUCE)
	for reducerIdLogic, placementWorker := range redBindings {
		println("reducer with logic ID: ", reducerIdLogic, " on worker : ", placementWorker)
		worker, err := core.GetWorker(placementWorker, &Workers) //get dest worker for the new Reduce Instance
		core.CheckErr(err, true, "reducerBindings in comunication")
		//instantiate the new reducer instance with expected calls # from mapper for termination and faultTollerant
		err = (worker).State.ControlRPCInstance.Client.Call("CONTROL.ActivateNewReducer", redExpectedCalls[reducerIdLogic], &redNewInstancePort)
		if core.CheckErr(err, false, "instantiating reducer: "+strconv.Itoa(reducerIdLogic)) {
			errsAtLeastOne = true
			endsRpc = append(endsRpc, InstanceRefRpcErrs{
				InstanceId: reducerIdLogic,
				Error:      err,
				ErrorKind:  REDUCER_ACTIVATE,
			})
		}
		reducerBindings[reducerIdLogic] = worker.Address + ":" + strconv.Itoa(redNewInstancePort) //note the binding to reducer correctly instantiated
	}
	/// RPC 5 ---> REDUCERS BINDINGS COMUNICATION TO MAPPERS
	//comunicate to all mappers final Reducer location

	//endsRpc:=make([]*rpc.Call,0,len(Workers.WorkersMapReduce)*len(Workers.WorkersMapReduce[0].State.WorkerIstances))
	for _, worker := range Workers.WorkersMapReduce {
		for _, instance := range worker.State.WorkerIstances {
			if instance.Kind == core.MAP {
				endsRpc = append(endsRpc, InstanceRefRpcErrs{
					WorkerId:   worker.Id,
					InstanceId: instance.Id,
					call:       instance.Client.Go("MAP.Map_quick_route_reducers", reducerBindings, &voidReply, nil),
				})
			}
		}
	}
	//wait rpc return
	for _, end := range endsRpc {
		<-end.call.Done
		if core.CheckErr(end.call.Error, false, "error on bindings comunication") {
			errsAtLeastOne = true
			end.Error = end.call.Error
			end.ErrorKind = MAPPER_COMUNICATION_REDUCERS
		}
	}
	if errsAtLeastOne {
		return endsRpc
	}
	return nil
}
func waitJobsEnd() {
	/*
		block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
	*/
	for r := 0; r < core.Config.ISTANCES_NUM_REDUCE; r++ {
		<-reducerEndChan //wait end of all reducers
	}
	//TODO KILL ALL WORKERS by HEARBIT special SIG
}
func cleanUpResidueWorkers() {
	/*
		kill mapper not ended because of possibility resend info to restarted reducer (if eventually failed)
	*/
}

///////////////////// OLD VERSION FOR REFERENCE	//////////////////

/*
func _old_main_wrapper() { //TODO OLD
	/*var filenames []string = []string{"/home/andysnake/GolandProjects/mapreducego/txtSrc/1012-0.txt"}
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}
	core.ReadConfigFile()
	defTokens := _main(filenames) //MAP&REDUCE HERE
	if Config.SORT_FINAL {
		/// SORTING LIST ..see https://golang.org/pkg/sort/
		tks := tokenSorter{defTokens}
		sort.Sort(sort.Reverse(tks))
	}
	//fmt.Println(&tks.tokens,&outToken,&tks.tokens==&outToken)
	serializeToFile(defTokens, OUTFILENAME)
	os.Exit(0)
}

func _main(filenames []string) []Token {
	//main payload , return final processed tokens ready to be serialized

	OpenedFiles = make([]*os.File, len(filenames))
	chunks := _init_chunks(filenames)

	//initialize the max num of required workers for map and reduce phases
	_workerNum := max(Config.WORKER_NUM_MAP, Config.ISTANCES_NUM_REDUCE)
	Workers = workersInit(_workerNum)

	//cleanUp files opened before
	cleanUpFiles(OpenedFiles)

	fmt.Println("SUCCESSFULLY RYSED: ", _workerNum, " workers!")
	///		MAP PHASE		/////////
	fmt.Println("---		MAP PHASE		---")
	mapResoults := assignWorks_map(chunks) //reqeust and collect MAP ops via rpc
	//TODO OLD terminate workers thread overneeded after map phase
	//if Config.WORKER_NUM_MAP > Config.ISTANCES_NUM_REDUCE {
	//	for x := 0; x < max(0, Config.WORKER_NUM_MAP-Config.ISTANCES_NUM_REDUCE); x++ {
	//		Workers[_workerNum-1-x].terminate <- true //terminate Worker x using a bool chan
	//	}
	//	Workers = Workers[:Config.ISTANCES_NUM_REDUCE]
	//}

	///	SHUFFLE & SORT_FINAL PHASE 	//////////////////
	fmt.Println("---		SHUFFLE & SORT_FINAL PHASE		---")
	tokenAll := mergeToken(mapResoults)

	///	REDUCE PHASE	/////////////////////
	fmt.Println("---		REDUCE PHASE		---")
	defTokens, err := assignWorks_Reduce(tokenAll)
	if err != nil {
		log.Println(err)
		os.Exit(95)
	}
	//
	//for _, Worker := range Workers { //terminate residue Worker thread when reduce phases has compleated
	//	//Worker.terminate <- true
	//}
	return defTokens

}*/
/*func assignWorks_map(chunks []CHUNK) []map[string]int {

		handle data chunk assignment to map Worker
		only a chunk will be assigned to a map rpc server by async rpc
		rpc server are simulated in thread
		map termiate when all rpc requests have received an answer


	///MAP RPC CONNECTION
	// Try to connect master to rpc map servers previously  rysed
	//support vectors for rpc calls and replys

	replys := make([]map[string]int, Config.WORKER_NUM_MAP)
	divCalls := make([]*rpc.Call, Config.WORKER_NUM_MAP)
	/// ASYNC RPC CALLS TO WORKERS !!!!
	for x := 0; x < len(chunks); x++ {
		divCalls[x] = Workers[x].client.Go("Map.Map_parse_builtin", chunks[x], &replys[x], nil)
	}
	///	COLLECT RPC MAP RESULTS
	outMapRes := make([]map[string]int, Config.WORKER_NUM_MAP)
	//wait for rpc calls completed
	for z := 0; z < len(divCalls); z++ {
		divCall := <-divCalls[z].Done //block until rpc map call z has completed
		checkErr(divCall.Error, true,"")
		outMapRes[z] = replys[z]
	}
	return outMapRes
}

func mergeToken(tokenList []map[string]int) map[string][]int {
	//merge map results grouping by keys
	//return special map (Key->Values) for reduce works assigns

	outTokenGrouped := make(map[string][]int)
	//merge tokens produced by mappers
	for x := 0; x < len(tokenList); x++ {
		for k, v := range tokenList[x] {
			outTokenGrouped[k] = append(outTokenGrouped[k], v) //append Values of dict_x to proper Key group
		}
	} // out Token now contains all Token obtained from map works

	return outTokenGrouped
}



func assignWorks_Reduce(tokensMap map[string][]int) ([]Token, error) {
	//assign reduce works to Reduce workers,collecting result will be built final token list

	workerCallCounters := make([]int, Config.ISTANCES_NUM_REDUCE) //keep track of reduce call num per reduce Worker ..to collect result

	var totKeys = len(tokensMap)
	fmt.Println("reducing Middle values #:=", totKeys)
	avgCallPerWorkerFair := len(tokensMap) / Config.ISTANCES_NUM_REDUCE
	rpcs_var_wrapped := make([][]RPCAsyncWrap, Config.ISTANCES_NUM_REDUCE) //rpc struct to handle all rpc calls on collection
	//////	rpc links setup
	for x := 0; x < Config.ISTANCES_NUM_REDUCE; x++ {
		rpcs_var_wrapped[x] = make([]RPCAsyncWrap, avgCallPerWorkerFair)
		workerCallCounters[x] = 0
	}
	//reduce work assignement to workers can be done by hash function or RR policy
	var destWorker int //destination Worker ID for reduction of values of a key
	var c int = 0      //counter for RR assignement

	////////	INVOKE RPC REDUCE CALLS...
	for k, v := range tokensMap {
		//compute destination Worker ID
		//destWorker = hashKeyReducerSum(k, Config.ISTANCES_NUM_REDUCE)		//hash of key assignement
		destWorker = c % int(Config.ISTANCES_NUM_REDUCE)
		c++
		arg := ReduceArg{Key: k, Values: v}
		//evalutate to extend rpc struct num ...only needed with hash assignement
		callsNumOfWorker := workerCallCounters[destWorker]         //ammount of rpc call done on destWorker
		if callsNumOfWorker >= len(rpcs_var_wrapped[destWorker]) { //EXTEND SPACE OF REPLY MATRIX ON NEED
			rpcs_var_wrapped[destWorker] = append(rpcs_var_wrapped[destWorker], RPCAsyncWrap{})
		}
		rpcWrap_k := &rpcs_var_wrapped[destWorker][callsNumOfWorker] //istance of struct for rpc of Key k
		//RPC CALL
		rpcWrap_k.divCall = Workers[destWorker].client.Go("Reduce.Reduce_tokens_key", arg, &rpcWrap_k.Reply, nil)
		workerCallCounters[destWorker]++
	}

	//////			COLLECT REDUCE RESULTs	////
	out
	s := make([]Token, len(tokensMap)) //all result list
	var tokenCounter int64 = 0
	//iterate among rpcWrappers structures and append result in final Token list
	//block for each scheduled rpc , set result on outTokens using a counter
	for z := 0; z < len(rpcs_var_wrapped); z++ { //iterate among workers
		for y := 0; y < workerCallCounters[z]; y++ { //to their effectively calls done (initial allocation is stimed by an fair division)
			rpcWraped := &rpcs_var_wrapped[z][y]
			done := <-rpcWraped.divCall.Done //block until rpc compleated
			checkErr(done.Error, true,"")
			//append result of this terminated rpc
			outTokens[tokenCounter] = rpcWraped.Reply
			tokenCounter++
		}
	}
	return outTokens, nil
}
*/
