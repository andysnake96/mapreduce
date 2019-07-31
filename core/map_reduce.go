package core

//core functions of map reduce,
//different map and reduce method for map and reduce types
import (
	"net/rpc"
	"strings"
	"sync"
	"unicode"
)

type Mapper MapperIstanceStateInternal
type Reducer ReducerIstanceStateInternal

func (c *WorkerChunks) Get_chunk_ids(chunkIDs []int, reply *int) error {
	//TODO CHECK sync.Map doc for auto handling these issuses
	chunksDownloaded := make([]CHUNK, len(chunkIDs))
	barrierDownload := new(sync.WaitGroup)
	barrierDownload.Add(len(chunkIDs))
	for i, chunkId := range chunkIDs {
		//check if chunk already downloaded
		if getChunk(chunkId, c) != CHUNK("") {
			println("already have chunk Id :", chunkId)
			continue
		}
		go downloadChunk(chunkId, &barrierDownload, &chunksDownloaded[i]) //download chunk from data store and save in isolated position
	}
	barrierDownload.Wait() //wait end of concurrent downloads
	//settings downloaded chunksDownloaded in datastore
	for i, chunkId := range chunkIDs { //chunkIDS and chunksDownloaded has same indexing semanting
		c.Chunks[chunkId] = chunksDownloaded[i]
	}
	return nil
}

func downloadChunk(chunkId int, waitGroup **sync.WaitGroup, chunkLocation *CHUNK) {
	/*
		download chunk from data store, allowing concurrent download with waitgroup to notify downloads progress
		chunk will be written in given location, thread safe if chunkLocation is isolated and readed only after waitgroup has compleated
	*/
	if Config.LOCAL_VERSION {
		chunk, present := ChunksStorageMock[chunkId]
		if !present {
			panic("NOT PRESENT CHUNK IN MOCK") //TODO ROBUSTENESS PRE EBUG
		}
		*chunkLocation = chunk //write chunk to his isolated position
	} //else //TODO DOWNLOAD FROM S3, CONFIG FILE AND S3 SDK.... only mem--> S3 rest
	(*waitGroup).Done() //notify other chunk download compleated
}

///// 		MAP		/////////////////

func (m *Mapper) Map_parse_builtin_quick_route(rawChunkId int, destinationsCosts *map[int]int) error {
	/*

		map operation over rawChunck resolved from his Id
		chunk readed will be splitted in word and pre groupped by key using an hashmap (COMBINER function embedded)
		for the locality aware routing of the next phase will be returned to master info about Mapper association to Reducer node
		( will be selected Reducers positioning considering data locality, also minimizing net Overhead )

	*/
	rawChunk := getChunk(rawChunkId, m.workerChunks) //TODO
	m.IntermediateTokens = make(map[string]int)
	*destinationsCosts = make(map[int]int) //cost of routing to Reducers for intermediate tokens
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
		(*destinationsCosts)[destReducerNodeId] += estimateTokenSize(Token{k, v})
	}
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
func (m *Mapper) Map_quick_route_reducers(reducersAddresses map[int]string) error {
	/*
		master will comunicate how to route intermediate tokens to reducers by this RPC
		reducersAddresses binds reducers ID to their actual Address
		selected by the master minimizing the network overhead exploiting the data locallity on Mapper nodes
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
	r.CumulativeCalls++ //update cummulative calls
	println(*mapperIDTODO, "TODO DEBUG")
	for _, token := range intermediateTokens {
		//cumulate intermediate token values in a cumulative dictionary
		r.IntermediateTokensCumulative[token.K] += token.V
	}
	if r.CumulativeCalls == r.ExpectedRedCalls {
		//go TODO RPC TO returnReduce(r.intermediateTokensCumulative)

	}
	return nil
}

////////		MASTER		///////////////////////////////
type _master int

var FinalTokens []Token //TODO LINK FROM GLOBAL ONE, TODO SMART PREALLOCATION

func (master *_master) returnReduce(finalTokensPartial map[string]int) error {
	for k, v := range finalTokensPartial {
		FinalTokens = append(FinalTokens, Token{k, v})
	}
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
