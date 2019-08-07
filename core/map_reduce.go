package core

//core functions of map reduce,
//different map and reduce method for map and reduce types
import (
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

type Mapper MapperIstanceStateInternal   //map
type Reducer ReducerIstanceStateInternal //reduce

func downloadChunk(chunkId int, waitGroup **sync.WaitGroup, chunkLocation *CHUNK) {
	/*
		download chunk from data store, allowing concurrent download with waitgroup to notify downloads progress
		chunk will be written in given location, thread safe if chunkLocation is isolated and readed only after waitgroup has compleated
	*/
	if Config.LOCAL_VERSION {
		chunk, present := ChunksStorageMock[chunkId]
		if !present {
			panic("NOT PRESENT CHUNK IN MOCK\nidchunk: " + strconv.Itoa(chunkId)) //TODO ROBUSTENESS PRE EBUG
		}
		*chunkLocation = chunk //write chunk to his isolated position
	} //else //TODO DOWNLOAD FROM S3, CONFIG FILE AND S3 SDK.... only mem--> S3 rest
	(*waitGroup).Done() //notify other chunk download compleated
}

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
}
func (workerNode *Worker_node_internal) ActivateNewReducer(expectedNumCallsFromMappers map[int]int, chosenPort *int) error {
	//create a new worker instance on top of workerNode, returning the chosen port for the new instance
	*chosenPort = NextUnassignedPort(Config.REDUCE_SERVICE_BASE_PORT, &AssignedPortsAll, true, true)
	redInitData := GenericInternalState{ReduceData: ReducerIstanceStateInternal{ExpectedRedCalls: expectedNumCallsFromMappers}}
	err, _ := InitRPCWorkerIstance(&redInitData, *chosenPort, REDUCE, workerNode)
	return err
}

type Map2ReduceRouteCost struct {
	RouteCosts map[int]int //for each reducerID (logic) --> cumulative data routing cost
	RouteNum   map[int]int //for each reducerID (logic) --> expected num calls reduce()
}

///// 		MAP		/////////////////
func (m *Mapper) Map_parse_builtin_quick_route(rawChunkId int, destinationsCosts *Map2ReduceRouteCost) error {
	/*

		map operation over rawChunck resolved from his Id
		chunk readed will be splitted in word and pre groupped by key using an hashmap (COMBINER function embedded)
		for the locality aware routing of the next phase will be returned to master info about Mapper association to Reducer node
		( will be selected Reducers positioning considering data locality, also minimizing net Overhead )

	*/
	m.ChunkID = rawChunkId
	rawChunk := getChunk(rawChunkId, m.WorkerChunks)
	m.IntermediateTokens = make(map[string]int)
	*destinationsCosts = Map2ReduceRouteCost{
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
	//building reverse map for smart activations of ReducerNodes
	var destReducerNodeId int
	for k, v := range m.IntermediateTokens {
		destReducerNodeId = HashKeyReducerSum(k, Config.ISTANCES_NUM_REDUCE)
		(*destinationsCosts).RouteCosts[destReducerNodeId] += estimateTokenSize(Token{k, v})
		(*destinationsCosts).RouteNum[destReducerNodeId]++
	}
	return nil
}
func (m *Mapper) Map_quick_route_reducers(reducersAddresses map[int]string, voidReply *int) error {
	/*
		master will communicate how to route intermediate tokens to reducers by this RPC
		reducersAddresses binds reducers ID to their actual Address
		selected by the master minimizing the network overhead exploiting the data locality on Mapper nodes
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
func InitRpcClients(addresses map[int]string) (map[int]*rpc.Client, error) {
	clients := make(map[int]*rpc.Client, len(addresses))
	var err error
	for k, v := range addresses {
		clients[k], err = rpc.Dial(Config.RPC_TYPE, v)
		if CheckErr(err, false, "dialing") {
			return nil, err
		}
	}
	return clients, nil
}

func estimateTokenSize(token Token) int {
	//return unsafe.Sizeof(token.V)+unsafe.Sizeof(token.K[0])*len(token.K)	//TODO CAST ERR
	return len(token.K) + 4
}

func getChunk(chunkID int, workerChunksStore *WorkerChunks) CHUNK {
	//TODO GET CHUNK FROM CHUNKS DOWNLOADED void chunk if not present in chunks store
	//because of go map concurrent access allowing has variated among different version a mutex will be taken for the access
	workerChunksStore.Mutex.Lock()
	chunk, present := workerChunksStore.Chunks[chunkID]
	workerChunksStore.Mutex.Unlock()
	if !present {
		return CHUNK("")
	}
	return chunk
}

////////// 		REDUCE 		///////////////

type ReduceArg struct {
	//reduce argument for Reduction list of Values of a Key
	Key    string
	Values []int
}

func (r *Reducer) Reduce_IntermediateTokens(intermediateTokens []Token, mapperIDTODO *int) error {
	return nil
}
func (r *Reducer) Reduce_(intermediateTokens []Token, mapperIDTODO *int) error {
	//r.CumulativeCalls++ //update cummulative calls
	//println(*mapperIDTODO, "TODO DEBUG")
	//for _, token := range intermediateTokens {
	//	//cumulate intermediate token values in a cumulative dictionary
	//	r.IntermediateTokensCumulative[token.K] += token.V
	//}
	//if r.CumulativeCalls == r.ExpectedRedCalls {
	//	//go TODO RPC TO returnReduce(r.intermediateTokensCumulative)
	//
	//}
	return nil
}

////////		MASTER		///////////////////////////////
type Master struct {
	FinalTokens     []Token
	mutex           sync.Mutex
	ReturnedReducer chan bool
}

func (master *Master) ReturnReduce(finalTokensPartial map[string]int, voidReply *int) error {
	master.mutex.Lock()
	for k, v := range finalTokensPartial {
		master.FinalTokens = append(master.FinalTokens, Token{k, v})
	}
	master.mutex.Unlock()
	master.ReturnedReducer <- true //notify returned reducer
	return nil
}

///////////////////////////////////////////////////TODO OLD VERSIONS
//map
func (m Mapper) Map_parse_builtin(rawChunck string, tokens *map[string]int) error {
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
func (m *Mapper) Map_string_builtin(rawChunck string, tokens *map[string]int) error { //TODO DEPRECATED
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
func (m *Mapper) Map_raw_parse(rawChunck string, tokens *map[string]int) error { //TODO DEPRECATED
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

//reduce
func (r *Reducer) Reduce_tokens_key(args ReduceArg, outToken *Token) error { //TODO OLD VERSION
	//version indicated in paper
	//reduce op by Values of a single Key
	//return final Token with unique Key string
	count := 0
	for x := 0; x < len(args.Values); x++ {
		count += args.Values[x]
	}
	*outToken = Token{args.Key, count}
	return nil
}
