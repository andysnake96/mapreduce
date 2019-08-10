package core

import (
	"errors"
	"net/rpc"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

//core functions of map reduce
//control rpc used to trigger MAP and REDUCE calls over worker instances over worker nodes

/////	CONTROL-RPC	/////////////////
func (workerNode *Worker_node_internal) Get_chunk_ids(chunkIDs []int, reply *int) error {
	sort.Ints(chunkIDs)
	GenericPrint(chunkIDs)
	chunksDownloaded := make([]CHUNK, len(chunkIDs))
	barrierDownload := new(sync.WaitGroup)
	barrierDownload.Add(len(chunkIDs))
	for i, chunkId := range chunkIDs {
		//check if chunk already downloaded
		if getChunk(chunkId, &(workerNode.WorkerChunksStore)) != CHUNK("") {
			println("already have chunk Id :", chunkId)
			barrierDownload.Add(-1)
			continue
		}
		go downloadChunk(chunkId, &barrierDownload, &chunksDownloaded[i]) //download chunk from data store and save in isolated position
	}
	barrierDownload.Wait() //wait end of concurrent downloads
	//settings downloaded chunksDownloaded in datastore
	for i, chunkId := range chunkIDs { //chunkIDS and chunksDownloaded has same indexing semanting
		workerNode.WorkerChunksStore.Chunks[chunkId] = chunksDownloaded[i]
	}
	return nil
}

func (workerNode *Worker_node_internal) RemoteControl_NewInstance(instanceKind int, chosenPort *int) error {
	//create a new worker instance on top of workerNode, returning the chosen port for the new instance
	var dfltPort int
	if instanceKind == CONTROL {
		dfltPort = Config.CHUNK_SERVICE_BASE_PORT
	} else if instanceKind == MAP {
		dfltPort = Config.MAP_SERVICE_BASE_PORT
	} else if instanceKind == REDUCE {
		dfltPort = Config.REDUCE_SERVICE_BASE_PORT
	}
	*chosenPort = NextUnassignedPort(dfltPort, &AssignedPortsAll, true, true)
	//if Config.LOCAL_VERSION {
	//} //else { worker node level assigned ports
	err, _ := InitRPCWorkerIstance(nil, *chosenPort, instanceKind, workerNode) //init instance propagating errors
	return err
} //TODO OTHER BRANCH
func (workerNode *Worker_node_internal) ActivateNewReducer(numChunks int, chosenPort *int) error {
	//create a new reducer actual instance on top of workerNode, returning the chosen port for the new instance
	*chosenPort = NextUnassignedPort(Config.REDUCE_SERVICE_BASE_PORT, &AssignedPortsAll, true, true)
	masterClient, err := rpc.Dial(Config.RPC_TYPE, Addresses.Master+":"+strconv.Itoa(Config.MASTER_BASE_PORT)) //init master client for future final return
	if CheckErr(err, false, "reducer activation master link fail") {
		return err
	}
	//init expected intermdiate data shares for the new reducer
	cumulativesCalls := make(map[int]bool)
	for i := 0; i < numChunks; i++ {
		cumulativesCalls[i] = false
	}
	redInitData := GenericInternalState{ReduceData: ReducerIstanceStateInternal{
		IntermediateTokensCumulative: make(map[string]int),
		CumulativeCalls:              cumulativesCalls,
		mutex:                        sync.Mutex{},
		MasterClient:                 masterClient,
	},
	}
	err, newReducerId := InitRPCWorkerIstance(&redInitData, *chosenPort, REDUCE, workerNode)
	println("activated new reducer:", newReducerId)
	return err
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

func (workerNode *Worker_node_internal) DoMAPs(MapChunkIds []int, DestinationsCosts *Map2ReduceRouteCost) error {
	/*
		for each chunk assigned to this worker concurrent map operation will be started
		intermediate tokens will be aggregated at worker level
		routing cost of this intermediate data to reducers will be returned to master as well eventual errors
	*/
	destCostOut := make([]chan Map2ReduceRouteCost, len(MapChunkIds))
	initiatedInstances := make([]*WorkerInstanceInternal, len(MapChunkIds))
	var newInstance *WorkerInstanceInternal
	for i, chunkId := range MapChunkIds {
		newInstance = workerNode.initLogicWorkerIstance(nil, MAP, &chunkId) //init new mapper logic instance with chunkID as instance id
		initiatedInstances[i] = newInstance
		destCostOut[i] = make(chan Map2ReduceRouteCost)
		go newInstance.IntData.MapData.Map_parse_builtin_quick_route(chunkId, &(destCostOut[i])) //concurrent map computations
	}
	*DestinationsCosts = Map2ReduceRouteCost{
		RouteCosts: make(map[int]int, Config.ISTANCES_NUM_REDUCE),
		RouteNum:   make(map[int]int, Config.ISTANCES_NUM_REDUCE),
	}
	for _, destCostChan := range destCostOut { //aggreagate mappers routings listening on passed channels
		mapperDestCosts := <-destCostChan
		routeInfosCombiner(mapperDestCosts, DestinationsCosts)
	}
	///	per reducer aggregated intermediate tokens to reduce to final tokens
	workerNode.IntermediateDataAggregated = AggregatedIntermediateTokens{
		ChunksSouces:                 MapChunkIds,
		PerReducerIntermediateTokens: make([]map[string]int, Config.ISTANCES_NUM_REDUCE),
	}
	//init aggregation var
	for i := 0; i < Config.ISTANCES_NUM_REDUCE; i++ {
		workerNode.IntermediateDataAggregated.PerReducerIntermediateTokens[i] = make(map[string]int)
	}
	for _, instance := range workerNode.Instances {
		if instance.ControlData.Kind == MAP {
			if len(instance.IntData.MapData.IntermediateTokens) < 5 {
				panic("fuck") //TODO DEBUG
			}
			for key, value := range instance.IntData.MapData.IntermediateTokens {
				destReducer := HashKeyReducerSum(key, Config.ISTANCES_NUM_REDUCE)
				workerNode.IntermediateDataAggregated.PerReducerIntermediateTokens[destReducer][key] += value //aggregate token per destination reducer
			}
		}
	}
	return nil
}

func (workerNode *Worker_node_internal) ReducersCollocations(ReducersAddresses map[int]string, Errs *[]error) error {
	/*
		set rpc connection clients to reducer located from the master to worker nodes exploiting data locality among workers
		async rpc reduce calls propagating to caller eventual errors occurred during connections or reduce calls
		wrapped errors in list structurated over constant sub parts for fault recovery
	*/
	*Errs = make([]error, 0)
	hasErrd := false
	var err error
	///	setup reducer connections
	for reducerId, reducerFinalAddress := range ReducersAddresses {
		workerNode.ReducersClients[reducerId], err = rpc.Dial(Config.RPC_TYPE, reducerFinalAddress)
		if CheckErr(err, false, "dialing reducer") {
			hasErrd = true
			*Errs = append(*Errs, errors.New(REDUCE_CONNECTION+ERROR_SEPARATOR+strconv.Itoa(reducerId)))
		}
	}
	if hasErrd {
		return errors.New("setUp Clients error")
	}

	///	reduce calls over aggregated intermediate tokens
	ends := make([]*rpc.Call, Config.ISTANCES_NUM_REDUCE)
	sourcesChunks := workerNode.IntermediateDataAggregated.ChunksSouces
	for reducerLogicId, intermediateTokens := range workerNode.IntermediateDataAggregated.PerReducerIntermediateTokens {
		ends[reducerLogicId] = workerNode.ReducersClients[reducerLogicId].Go("REDUCE.Reduce", ReduceArgs{intermediateTokens, sourcesChunks}, nil, nil)
	}
	for reducerLogicId, end := range ends {
		<-end.Done
		if end.Error != nil {
			hasErrd = true
			*Errs = append(*Errs, errors.New(REDUCE_CALL+ERROR_SEPARATOR+strconv.Itoa(reducerLogicId)))
		}
	}
	if hasErrd {
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
		    intermediate tokens stored in mapper field
			route destination returned to caller via Go channel
	*/
	m.ChunkID = rawChunkId
	rawChunk := getChunk(rawChunkId, m.WorkerChunks)
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
		panic("intermdiate token error") //TODO DEBUG
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
		master will communicate how to route intermediate tokens to reducers by this RPC
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
	//for i,tokens:=tokenPerReducer{
	//	reducersCall[i]:=clients[i].Go("REDUCE.reduce",tokens,nil,nil)
	//}
	//for _,done:=range reducersCall{
	//	<-done.Done
	//}
	return nil
}

////////// 		REDUCE 		///////////////
type ReduceArgs struct {
	IntermediateTokens map[string]int //key-value pre aggregated at worker lev
	Source             []int          //intermediate tokens source  ( for failure error )
}

func (r *ReducerIstanceStateInternal) Reduce(RedArgs ReduceArgs, voidReply *int) error {
	/*
		reduce function, aggregate intermediate tokens in final tokens with fault tollerant
		cumulative variable protected by a mutex because of possible race condition over simultaneously rpc dispached to reducer
		intermediate inputs buffered at reduce levele for fault recovery ( avoid all reduce call to re-happend)
	*/
	r.mutex.Lock() //avoid race condition over cumulatives variable
	//update cumulative calls per intermdiate data share (chunk's derivate)
	duplicateIntermdiateData := false
	for _, chunkId := range RedArgs.Source {
		if r.CumulativeCalls[chunkId] == true {
			duplicateIntermdiateData = true //already processed that intermdiate token share
			println("intermdiate data contains a share:", chunkId, "already processed")
		} else if duplicateIntermdiateData { //intermdiate data id collision--->something already processed something not!
			panic("CRITICAL INTERMDIATE DATA COLLISION ... ABORTING")
		} else {
			r.CumulativeCalls[chunkId] = true //set that intermediate tokens share has being received
		}
	}
	if duplicateIntermdiateData {
		r.mutex.Unlock()
		return nil
	}
	//actual reduce logic
	for key, value := range RedArgs.IntermediateTokens {
		r.IntermediateTokensCumulative[key] += value //continusly aggregate received intermediate tokens
	}
	//exit condition check
	allEnded := true
	for _, processedIntermdDataShare := range r.CumulativeCalls {
		if !processedIntermdDataShare { //check if all expected itermediate data share has been REDUCED()
			allEnded = false
		}
	}
	if allEnded {
		println("ALL ENDED at reducer :", r)
		err := r.MasterClient.Call("MASTER.ReturnReduce", r.IntermediateTokensCumulative, nil)
		CheckErr(err, false, "master return failed ") //TODO MASTER FAIL POCO PRIMA DI ULTIME REDUCE END
	}
	r.mutex.Unlock()
	return nil
}

////////		MASTER		///////////////////////////////
type Master struct {
	FinalTokens     []Token
	Mutex           sync.Mutex
	ReturnedReducer *chan bool
}

func (master *Master) ReturnReduce(FinalTokensPartial map[string]int, VoidReply *int) error {
	master.Mutex.Lock()
	for k, v := range FinalTokensPartial {
		master.FinalTokens = append(master.FinalTokens, Token{k, v})
	}
	*master.ReturnedReducer <- true //notify returned reducer
	master.Mutex.Unlock()
	return nil
}

///////////////////////////////////////////////////TODO OLD VERSIONS
//map
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
func (m *MapperIstanceStateInternal) Map_string_builtin(rawChunck string, tokens *map[string]int) error { //TODO DEPRECATED
	//map operation for a Worker, from assigned chunk string produce tokens Key V
	//used string builtin split ... performance limit... every chunk readed at least 2 times
	*tokens = make(map[string]int)
	//producing Token splitting rawChunck by \n and \b
	lines := strings.Split(rawChunck, "\n")
	const _WRD4LINE = 12
	words := make([]string, len(lines)*_WRD4LINE) //preallocate
	for _, l := range lines {
		words = append(words, strings.Split(l, " ")...) //TODO SLICES CONCATENATION GO PERFORMACE?
	}
	for _, w := range words {
		(*tokens)[w]++ //set Token in a dict to simply handle Key repetitions
	}
	return nil
}
func (m *MapperIstanceStateInternal) Map_raw_parse(rawChunck string, tokens *map[string]int) error { //TODO DEPRECATED
	//map op RPC for a Worker
	// parse a chunk in words avoiding to include dotting marks \b,:,?,...
	*tokens = make(map[string]int)
	dotting := map[byte]bool{'.': true, ',': true, ';': true, '-': true, ':': true, '?': true, '!': true, '\n': true, '\r': true, ' ': true, '"': true}
	//parser states
	const STATE_WORD = 0
	const STATE_NOTWORD = 1
	state := STATE_WORD
	var char byte
	//words low delimiter index in chunk
	wordDelimLow := 0
	//set initial state
	char = rawChunck[0] //get first char
	if dotting[char] {
		state = STATE_NOTWORD
	}
	//PRODUCING OUTPUT TOKEN HASHMAP IN ONLY 1! READ OF chunk chars...
	var isWordChr bool                    //bool true if actual char is word
	for i := 0; i < len(rawChunck); i++ { //iterate among chunk chars
		char = rawChunck[i]
		isWordChr = !(dotting[char])
		if state == STATE_WORD && !isWordChr { //split condition
			word := rawChunck[wordDelimLow:i]
			(*tokens)[word]++ //set Token Key in out hashmap
			state = STATE_NOTWORD
		}
		//TODO ELIF LIKE... ALREADY EXCLUSIVE CONDITIONS
		if state == STATE_NOTWORD && isWordChr {
			wordDelimLow = i //set new word low index delimiter
			state = STATE_WORD
		}
	}
	//fmt.Println("DEBUG DICTIONARY OF TOKENS-->", *tokens)
	return nil
}