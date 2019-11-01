package core

import (
	"../aws_SDK_wrap"
	"errors"
	"net"
	"net/rpc"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const FINAL_TOKEN_FILENAME = "outTokens.list"

//core functions of map reduce
//control rpc used to trigger MAP and REDUCE calls over worker instances over worker nodes

/////	CONTROL-RPC	/////////////////
func (workerNode *Worker_node_internal) Get_chunk_ids(chunkIDs []int, voidReply *int) error {
	/*
		download from distributed storage, chunks corresponding  to passed chunksID
		it's safe to recall this function because will download only chunks not cached on worker node
		because of internal storage of chunks (go map) is not thread safe to call this function concurrently on same worker
		void return
	*/
	sort.Ints(chunkIDs)
	chunksDownloaded := make([]CHUNK, len(chunkIDs))
	chunksDownloadedErrors := make([]error, len(chunkIDs))
	barrierDownload := new(sync.WaitGroup)
	barrierDownload.Add(len(chunkIDs))
	for i, chunkId := range chunkIDs { //concurrent download of not cached chunks
		//check if chunk already downloaded
		_, chunksPresent := workerNode.WorkerChunksStore.Chunks[chunkId]
		if chunksPresent {
			println("already have chunk Id :", chunkId)
			barrierDownload.Add(-1)
		} else {
			go workerNode.downloadChunk(chunkId, &barrierDownload, &chunksDownloaded[i], &chunksDownloadedErrors[i]) //download chunk from data store and save in isolated position
		}
	}
	//println("wait download\t",workerNode.Id)
	barrierDownload.Wait() //wait end of concurrent downloads
	//check if some error occurred during download
	for _, err := range chunksDownloadedErrors {
		if err != nil {
			return err
		}
	}
	for indx, chunk := range chunksDownloaded {
		if len(chunk) < 5 {
			panic("INVALID DOWNLOAD")
		}
		workerNode.WorkerChunksStore.Chunks[chunkIDs[indx]] = chunk
	}
	return nil
}
func (workerNode *Worker_node_internal) downloadChunk(chunkId int, downloadBarrier **sync.WaitGroup, chunkLocation *CHUNK, errorLocation *error) {
	/*
		download chunk from data store, allowing concurrent download with waitgroup to notify downloads progress
		chunk will be written in given location, thread safe if chunkLocation is isolated and readed only after waitgroup has compleated
	*/

	chunkBuf := make([]byte, Config.CHUNK_SIZE)
	err := aws_SDK_wrap.DownloadDATA(workerNode.Downloader, Config.S3_BUCKET, strconv.Itoa(chunkId), &chunkBuf, false)
	if CheckErr(err, false, "downloading chunk: "+strconv.Itoa(chunkId)) {
		(*downloadBarrier).Done()
		*errorLocation = err
		return
	}
	*chunkLocation = CHUNK(chunkBuf)

	(*downloadBarrier).Done() //notify other chunk download compleated
	*errorLocation = nil
}

type ReduceActiveArg struct {
	NumChunks int
	LogicID   int
}

func (workerNode *Worker_node_internal) ActivateNewReducer(arg ReduceActiveArg, ChosenPort *int) error {
	//create a new reducer instance on top of workerNode, returning the chosen port for the new instance
	//Idempontent function, it's safe to Re Activate a Reducer on same worker and same reducer port will be returned

	/*if Config.SIMULATE_WORKERS_SLOW_DOWN {
		rndSleep := rand.Int63n(int64(MAX_RND_SLEEP))
		time.Sleep(time.Duration(rndSleep))
	}*/

	//// check if already instantiated reducer if it exist return his port
	for _, reducer := range workerNode.ReducerInstances {
		if reducer.LogicID == arg.LogicID {
			*ChosenPort = reducer.Port
			return nil
		}
	}
	/// if new reducer activation is needed init master link and rpc server on this node
	masterRpcAddr := workerNode.MasterAddr + ":" + strconv.Itoa(Config.MASTER_BASE_PORT)
	masterClient, err := rpc.Dial(Config.RPC_TYPE, masterRpcAddr) //init master client for future final return
	if CheckErr(err, false, "reducer activation master link fail on addr= "+masterRpcAddr) {
		return err
	}
	maxActivationTry := Config.ISTANCES_NUM_REDUCE
startNewReducer:
	*ChosenPort = NextUnassignedPort(Config.REDUCE_SERVICE_BASE_PORT, &AssignedPortsAll, true, true, "tcp")
	//init expected intermdiate data shares for the new reducer
	cumulativesCalls := make(map[int]bool)
	for i := 0; i < arg.NumChunks; i++ {
		cumulativesCalls[i] = false
	}

	l, e := net.Listen("tcp", ":"+strconv.Itoa(*ChosenPort))
	if CheckErr(e, false, "reducer port opening err ") && Config.LOCAL_VERSION && maxActivationTry > 0 { //rare race condition on port avaibility check, simply retry
		maxActivationTry--
		goto startNewReducer
	}
	newReducer := ReducerIstanceStateInternal{
		IntermediateTokensCumulative: make(map[string]int),
		CumulativeCalls:              cumulativesCalls,
		mutex:                        sync.Mutex{},
		MasterClient:                 masterClient,
		LogicID:                      arg.LogicID,
		StateChan:                    workerNode.StateChan,
		Server:                       rpc.NewServer(),
		Port:                         *ChosenPort,
	}

	err = newReducer.Server.RegisterName("REDUCE", &newReducer)
	CheckErr(err, true, "reduce rpc register")
	go newReducer.Server.Accept(l)
	workerNode.ReducerInstances = append(workerNode.ReducerInstances, newReducer)
	workerNode.StateChan <- uint32(REDUCE)
	//println("activated new reducer:", arg.LogicID)
	return err
}

func (r *ReducerIstanceStateInternal) Reduce(RedArgs ReduceArgs, voidReply *int) error {
	/*
		reduce function, aggregate intermediate Tokens in final Tokens with fault tollerant
		cumulative variable protected by a mutex because of possible race condition over simultaneously rpc dispached to reducer
		intermediate inputs buffered at reduce levele for fault recovery ( avoid all reduce call to re-happend)
	*/

	//random sleep on reduce
	if Config.SIMULATE_WORKERS_SLOW_DOWN {
		time.Sleep(time.Millisecond * 1000)
	}
	r.mutex.Lock() //avoid race condition over cumulatives variable
	//update cumulative calls per intermdiate data share (chunk's derivate)
	duplicateIntermdiateData := false
	for _, chunkId := range RedArgs.Source {
		if r.CumulativeCalls[chunkId] == true {
			duplicateIntermdiateData = true //already processed that intermdiate token share
			println("intermdiate data contains a share:", chunkId, "already processed")
		} else if duplicateIntermdiateData { //intermdiate data id collision--->something already processed something not!
			GenericPrint(RedArgs.Source, "received chunk IDs")
			panic("CRITICAL INTERMDIATE DATA COLLISION ... ABORTING")
		} else {
			r.CumulativeCalls[chunkId] = true //set that intermediate Tokens share has being received
		}
	}
	if duplicateIntermdiateData {
		r.mutex.Unlock()
		return nil
	}
	if r.LogicID == 0 || !Config.LOCAL_VERSION { //TODO DEBUG PRINT
		GenericPrint(RedArgs.Source, "reducer received chunks")
	}
	//actual reduce logic
	for key, value := range RedArgs.IntermediateTokens {
		r.IntermediateTokensCumulative[key] += value //continusly aggregate received intermediate Tokens
	}
	//exit condition check
	allEnded := true
	for _, processedIntermdDataShare := range r.CumulativeCalls {
		if !processedIntermdDataShare { //check if all expected itermediate data share has been REDUCED()
			allEnded = false
		}
	}
	if allEnded {
		println("ALL ENDED at reducer :", r.LogicID)
		err := r.MasterClient.Call("MASTER.ReturnReduce", ReduceRet{r.LogicID, r.IntermediateTokensCumulative}, nil)
		CheckErr(err, false, "master return failed ")
		r.StateChan <- uint32(IDLE) //reducer ended
	}
	r.mutex.Unlock()
	return nil
}
func (workerNode *Worker_node_internal) ExitWorker(VoidArg int, VoidRepli *int) error {
	//stupid rpc to let the worker exit ---> master knows that worker won't have anymore calls/jobs schedule
	println("GoodBye from worker : ", workerNode.Id)
	workerNode.ExitChan <- true
	return nil
}

type Map2ReduceRouteCost struct {
	RouteCosts map[int]int //for each reducerID (logic) --> cumulative data routing cost
	RouteNum   map[int]int //for each reducerID (logic) --> expected num calls reduce() //TODO HASH PERF DEBUG ONLY.
}
type MapWorkerArgsWrap struct {
	WorkerId   int
	Reply      Map2ReduceRouteCost
	End        *rpc.Call
	Err        error
	MapJobArgs MapWorkerArgs
}
type MapWorkerArgs struct {
	ChunkIds          []int //map jobs assigned chunks
	ChunkIdsFairShare []int
	Chunks            []CHUNK
}

func (workerNode *Worker_node_internal) AssignMaps(arg MapWorkerArgs, DestinationsCosts *Map2ReduceRouteCost) error {
	/*
		for each chunk assigned to this worker concurrent map operation will be started
		intermediate Tokens will be aggregated at worker level
		routing cost of this intermediate data to reducers will be returned to master as well eventual errors
		eventual multiple calls will cause re computation only if mapJobsTODO has not been processed before
	*/
	workerNode.StateChan <- uint32(MAP)
	/////////// filter away already did map jobs
	mapJobsTODO := make([]int, 0, len(arg.ChunkIds)) //map jobs assigned to this worker
	if Config.LoadChunksToS3 {
		err := workerNode.Get_chunk_ids(arg.ChunkIds, nil)
		if CheckErr(err, false, "") {
			workerNode.StateChan <- uint32(IDLE)
			return err
		}
	} else {
		/*SLOW_DOWN_MAX_MAP := 80 * time.Millisecond
		sleepFor := rand.Int63n(int64(SLOW_DOWN_MAX_MAP))
		sleepFor += 100
		if sleepFor > 1 {
			time.Sleep(time.Duration(sleepFor))
		}*/ ////TODO SLOW DOWN FOR DEBUG --> RANDOM KILL

		for i, chunkID := range arg.ChunkIds {
			//workerMapJobs.Chunks[i]=data.Chunks[chunkId]	TODO PREVIUSLY SETTED
			_, isAlreadyOwnedChunk := workerNode.WorkerChunksStore.Chunks[chunkID]
			if !isAlreadyOwnedChunk {
				workerNode.WorkerChunksStore.Chunks[chunkID] = arg.Chunks[i]
				mapJobsTODO = append(mapJobsTODO, chunkID)
				if len(arg.Chunks[i]) < 5 {
					panic("invalid chunk received ")
				}
			} else {
				println("already have chunk :", chunkID)
			}
		}
	}

	GenericPrint(mapJobsTODO, "maps job on "+strconv.Itoa(workerNode.ControlRpcInstance.Port))
	destCostOut := make([]chan Map2ReduceRouteCost, len(mapJobsTODO))
	//concurrent do map on go rountines
	for i, chunkId := range mapJobsTODO {
		newMapper := MapperIstanceStateInternal{
			IntermediateTokens: make(map[string]int),
			WorkerChunks:       &(workerNode.WorkerChunksStore),
			ChunkID:            chunkId,
		}
		workerNode.MapperInstances[chunkId] = newMapper
		destCostOut[i] = make(chan Map2ReduceRouteCost)
		go newMapper.Map_parse_builtin_quick_route(chunkId, &(destCostOut[i])) //concurrent map computations
	}
	*DestinationsCosts = Map2ReduceRouteCost{
		RouteCosts: make(map[int]int, Config.ISTANCES_NUM_REDUCE),
		RouteNum:   make(map[int]int, Config.ISTANCES_NUM_REDUCE),
	}
	/// mapper routine sync && outputs fetch :)
	for i, destCostChan := range destCostOut {
		mapperDestCosts := <-destCostChan
		relatedChunkID := mapJobsTODO[i] //out chan has same indezing of mapJobsToDo
		// update mapper dest cost out for eventual later master recovery //TODO BARRIER AND DIRECT DestinationCosts UPDATE IN PLACE IN MAP() NOT WORK...
		mapper := workerNode.MapperInstances[relatedChunkID]
		mapper.DestinationCosts = mapperDestCosts
		workerNode.MapperInstances[relatedChunkID] = mapper
	}
	//aggregate destination cost of individual fair share of chunk for master answer
	for _, chunkID := range arg.ChunkIdsFairShare {
		mapperDestCosts := workerNode.MapperInstances[chunkID].DestinationCosts
		routeInfosCombiner(mapperDestCosts, DestinationsCosts)
	}
	//println("::route infos-->",len(DestinationsCosts.RouteCosts),len(DestinationsCosts.RouteNum))
	//workerNode.aggregateIntermediateTokens(mapJobsTODO,*DestinationsCosts)

	//if Config.BACKUP_MASTER{
	//	workerNode.cacheLock.Unlock()
	//}
	workerNode.StateChan <- uint32(IDLE)
	return nil
}
func (workerNode *Worker_node_internal) RecoveryMapRes(chunkFairShare []int, DestinationsCosts *Map2ReduceRouteCost) error {
	// re aggregate destination cost for requested chunkIDs ( not including replication)
	destCosts := Map2ReduceRouteCost{
		RouteCosts: make(map[int]int, Config.ISTANCES_NUM_REDUCE),
		RouteNum:   make(map[int]int, Config.ISTANCES_NUM_REDUCE),
	}

	for _, chunkId := range chunkFairShare {
		_, isAlreadyOwnedChunk := workerNode.WorkerChunksStore.Chunks[chunkId]
		if !isAlreadyOwnedChunk {
			panic("invalid chunk ID" + strconv.Itoa(chunkId))
		}
		println("aggreagating map dest cost of size: ", len(workerNode.MapperInstances[chunkId].DestinationCosts.RouteCosts))
		routeInfosCombiner(workerNode.MapperInstances[chunkId].DestinationCosts, &destCosts)
	}
	*DestinationsCosts = destCosts
	return nil
}
func (workerNode *Worker_node_internal) RecoveryReduceResult(reducerID int, FinalTokens *ReduceRet) error {
	// return final result to new master if it exist
	var reducer *ReducerIstanceStateInternal
	for _, r := range workerNode.ReducerInstances {
		if r.LogicID == reducerID {
			reducer = &r
			for _, processed := range r.CumulativeCalls {
				if !processed {
					return errors.New("uncompleted reduction")
				}
			}
		}
	}
	if reducer != nil {
		*FinalTokens = ReduceRet{reducerID, reducer.IntermediateTokensCumulative}
		return nil
	}
	return errors.New("not founded reducer")
}

type ReduceTriggerArg struct {
	ReducersAddresses     map[int]string
	IndividualChunkShare  []int
	FailPostPartialReduce bool
}

func (workerNode *Worker_node_internal) postFailReduce(arg ReduceTriggerArg) []error {
	//on fault of some of mapper/reducers with some reduce triggered not aggreagated reduce call will be done
	//avoiding reducer panic for partial collision of aggregated data with already owned data

	//for each chunk get interm data to route to reducer separately
	println("post fail re-reduce")
	intermDataChunkLevAggreag := make([][]map[string]int, len(arg.IndividualChunkShare))
	for i, chunk := range arg.IndividualChunkShare {
		intermDataChunkLevAggreag[i] = workerNode.aggregateIntermediateTokens([]int{chunk}, false)
	}
	ends := make(map[int][]*rpc.Call, len(intermDataChunkLevAggreag)) //aggreagate rpc calls at reduce level for error setting
	errs := make([]error, 0, len(intermDataChunkLevAggreag))
	for j, intermDataPerReducer := range intermDataChunkLevAggreag {
		chunkID := arg.IndividualChunkShare[j]
		for reducerID, intermData := range intermDataPerReducer {

			ends[reducerID] = append(ends[reducerID], workerNode.ReducersClients[reducerID].Go("REDUCE.Reduce", ReduceArgs{intermData, []int{chunkID}}, nil, nil))
		}
	}

	for reducerLogicId, reducerEnds := range ends {
		for _, end := range reducerEnds {
			select {
			case <-end.Done:
			case <-time.After(TIMEOUT_PER_RPC):
				end.Error = errors.New(ERR_TIMEOUT_RPC)
			}

			if end.Error != nil {
				errs = append(errs, errors.New(REDUCE_CALL+ERROR_SEPARATOR+strconv.Itoa(reducerLogicId)))
			}
		}
	}
	return errs
}

func (workerNode *Worker_node_internal) ReducersCollocations(arg ReduceTriggerArg, Errs *[]error) error {
	/*
		set rpc connection clients to reducer located from the master to worker nodes exploiting data locality among workers
		async rpc reduce calls propagating to caller eventual errors occurred during both connections or reduce calls
		wrapped errors in list structurated over constant sub parts for fault recovery
		for fault tollerant will be triggered async rpc only to reducers in ReducersAddresses (that may be the respawned one)
	*/
	//if Config.BACKUP_MASTER{
	//	workerNode.cacheLock.Lock()
	//	//// check if same maps has been requested before
	//	if &(workerNode.reduceCache.reducerBindings)!=nil && MapsEq(ReducersAddresses,workerNode.reduceCache.reducerBindings){
	//		*Errs=workerNode.reduceCache.Errs
	//		workerNode.cacheLock.Unlock()
	//		if len(*Errs)>0{
	//			return errors.New("old fail report")
	//		}
	//		return nil
	//	}
	//}
	/*if Config.SIMULATE_WORKERS_SLOW_DOWN {
		time.Sleep(200 * time.Millisecond)
	}*/
	errs := make([]error, 0)
	hasErrd := false
	var err error
	///	setup reducer connections
	for reducerId, reducerFinalAddress := range arg.ReducersAddresses {
		workerNode.ReducersClients[reducerId], err = rpc.Dial(Config.RPC_TYPE, reducerFinalAddress)
		if CheckErr(err, false, "dialing reducer") {
			hasErrd = true
			errs = append(*Errs, errors.New(REDUCE_CONNECTION+ERROR_SEPARATOR+strconv.Itoa(reducerId)))
		} else {
			println("Set Up Connection to reducer at ", reducerFinalAddress) //clean ext log
		}
	}
	if hasErrd {
		return errors.New("setUp Clients error")
	}

	if arg.FailPostPartialReduce { //to avoid eventual I.D. collision use only base data aggregation level
		errs = workerNode.postFailReduce(arg)
		if len(errs) > 0 {
			*Errs = errs
			return errors.New("reduce failed")
		}
		return nil
	}

	///	reduce calls over aggregated intermediate Tokens on newly created reducers in ReducersAddresses
	workerNode.aggregateIntermediateTokens(arg.IndividualChunkShare, true)
	ends := make([]*rpc.Call, 0, len(arg.ReducersAddresses))
	sourcesChunks := workerNode.IntermediateDataAggregated.ChunksSouces
	for reducerLogicId, intermediateTokens := range workerNode.IntermediateDataAggregated.PerReducerIntermediateTokens {
		//avoid useless retrasmission of interm. data to reducers
		_, isReductionOrdered := arg.ReducersAddresses[reducerLogicId]
		if !isReductionOrdered {
			continue
		}
		//TODO DEBUG PRINT
		if reducerLogicId == 0 || !Config.LOCAL_VERSION {
			GenericPrint(sourcesChunks, "chunks sending to reducer: "+strconv.Itoa(reducerLogicId))
		}

		end := workerNode.ReducersClients[reducerLogicId].Go("REDUCE.Reduce", ReduceArgs{intermediateTokens, sourcesChunks}, nil, nil)
		ends = append(ends, end)
	}
	//GenericPrint(sourcesChunks, "SOURCE CHUNKS SENT TO REDUCERS")
	for reducerLogicId, end := range ends {
		select {
		case <-end.Done:
		case <-time.After(TIMEOUT_PER_RPC):
			end.Error = errors.New(ERR_TIMEOUT_RPC)
		}

		if end.Error != nil {
			hasErrd = true
			errs = append(errs, errors.New(REDUCE_CALL+ERROR_SEPARATOR+strconv.Itoa(reducerLogicId)))
		}
	}
	//if Config.BACKUP_MASTER{
	//	workerNode.reduceCache=ReduceOutputCache{
	//		reducerBindings: ReducersAddresses,
	//		Errs:            *Errs,
	//	}
	//	workerNode.cacheLock.Unlock()
	//}
	if hasErrd {
		*Errs = errs
		return errors.New("reduce failed")
	}
	return nil
}

///// 		MAP		/////////////////
func (m *MapperIstanceStateInternal) Map_parse_builtin_quick_route(rawChunkId int, destinationsCostsChan *chan Map2ReduceRouteCost) {
	/*
			map operation over rawChunck resolved from its Id
			chunk readed will be splitted in word and pre groupped by key using an hashmap (COMBINER function embedded)
			for the locality aware routing of the next phase will be returned to master info about mapper association to Reducer node
			( will be selected Reducers positioning considering data locality, also minimizing net Overhead )
		    intermediate Tokens stored in mapper field
			route destination returned to caller via Go channel
	*/
	m.ChunkID = rawChunkId
	m.WorkerChunks.Mutex.Lock()
	rawChunk := m.WorkerChunks.Chunks[rawChunkId]
	if len(rawChunk) < 5 {
		println(string(rawChunk))
		panic("fetched invalid chunk" + string(rawChunk))
	}
	m.WorkerChunks.Mutex.Unlock()
	//m.IntermediateTokens = make(map[string]int)
	destinationsCosts := Map2ReduceRouteCost{
		RouteCosts: make(map[int]int, Config.ISTANCES_NUM_REDUCE),
		RouteNum:   make(map[int]int, Config.ISTANCES_NUM_REDUCE),
	}
	///		parse words
	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	words := strings.FieldsFunc(string(rawChunk), f) //parse Go builtin by spaces
	//words:= strings.Fields(rawChunck)	//parse Go builtin by spaces
	for _, word := range words {
		m.IntermediateTokens[word]++
	}
	if len(m.IntermediateTokens) < 5 {
		panic("intermdiate token error:" + rawChunk) //TODO DEBUG
	}
	//building reverse map for smart activations of ReducerNodes
	var destReducerNodeId int
	for k, v := range m.IntermediateTokens {
		destReducerNodeId = HashKeyReducerSum(k, Config.ISTANCES_NUM_REDUCE)
		(destinationsCosts).RouteCosts[destReducerNodeId] += estimateTokenSize(Token{k, v})
		(destinationsCosts).RouteNum[destReducerNodeId]++
	}
	*destinationsCostsChan <- destinationsCosts //send route result into chan
	runtime.Goexit()
}
func (m *MapperIstanceStateInternal) Map_quick_route_reducers(reducersAddresses map[int]string, voidReply *int) error {
	/*
		master will communicate how to route intermediate Tokens to reducers by this RPC
		reducersAddresses binds reducers ID to their actual Address
		selected by the master minimizing the network overhead exploiting the data locality on MapperIstanceStateInternal nodes
	*/
	var destReducerNodeId int
	//TODO MUTED
	//var destReducerNodeAddress string
	//clients,err :=InitRpcClients(reducersAddresses)
	//  CheckErr(err,true,"dialing reducers")
	//TODO INIT FROM RECEIVED ADDRESSES :)))

	//init per reducer token list with an averge fair size
	tokenPerReducer := make(map[int][]Token)
	//reducersCall:=make([]*rpc.Call,len(reducersAddresses))
	fairTokenSizeLen := len(m.IntermediateTokens) / Config.ISTANCES_NUM_REDUCE
	for i := 0; i < Config.ISTANCES_NUM_REDUCE; i++ {
		tokenPerReducer[i] = make([]Token, fairTokenSizeLen)
	}

	//pre aggregate series of reduce calls
	for k, v := range m.IntermediateTokens {
		destReducerNodeId = HashKeyReducerSum(k, Config.ISTANCES_NUM_REDUCE)
		tokenPerReducer[destReducerNodeId] = append(tokenPerReducer[destReducerNodeId], Token{k, v})
		//destReducerNodeAddress = reducersAddresses[destReducerNodeId]
	}
	//TODOrpc calls
	//i:=0
	//for i,Tokens:=tokenPerReducer{
	//	reducersCall[i]:=clients[i].Go("REDUCE.reduce",Tokens,nil,nil)
	//}
	//for _,done:=range reducersCall{
	//	<-done.Done
	//}
	return nil
}

////////// 		REDUCE 		///////////////
type ReduceArgs struct {
	IntermediateTokens map[string]int //key-value pre aggregated at worker lev
	Source             []int          //intermediate Tokens source  ( for failure error )
}
type ReduceRet struct {
	ReducerLogicID   int
	AggregatedTokens map[string]int
}

////////		MASTER		///////////////////////////////

type MasterRpc struct {
	FinalTokens      []Token
	Mutex            sync.Mutex
	ReturnedReducer  *chan bool
	ReducersReturned map[int]bool
}

func (master *MasterRpc) ReturnReduce(AggregatedTokensReducer ReduceRet, VoidReply *int) error {
	master.Mutex.Lock()
	_, alreadyReturnedReducer := (*master).ReducersReturned[AggregatedTokensReducer.ReducerLogicID]
	if alreadyReturnedReducer {
		master.Mutex.Unlock()
		println("reducer returned twice, ignoring data of size:", len(AggregatedTokensReducer.AggregatedTokens))
		return nil
	}
	for k, v := range AggregatedTokensReducer.AggregatedTokens {
		master.FinalTokens = append(master.FinalTokens, Token{k, v})
	}
	(*master).ReducersReturned[AggregatedTokensReducer.ReducerLogicID] = true
	*master.ReturnedReducer <- true //notify returned reducer
	println("Reduce returned, Collected new ", len(AggregatedTokensReducer.AggregatedTokens), " aggregated tokens")
	master.Mutex.Unlock()
	return nil
}
func (m MapperIstanceStateInternal) Map_parse_builtin(rawChunck string, tokens *map[string]int) error {
	*tokens = make(map[string]int)
	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	words := strings.FieldsFunc(rawChunck, f) //parse Go builtin by spaces
	//words:= strings.Fields(rawChunck)	//parse Go builtin by spaces
	for _, word := range words {
		(*tokens)[word]++
	}
	return nil
}
