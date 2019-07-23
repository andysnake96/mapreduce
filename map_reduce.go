package main

//core functions of map reduce,
//different map and reduce method for map and reduce types
import (
	"reflect"
	"strings"
	"unicode"
	"unsafe"
)

// 		MAP		/////////////////////7
type _map int //type map methods interface

func (m *_map) Map_parse_builtin(rawChunck string, tokens *map[string]int) error {
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


func (m *_map) Map_parse_builtin_quick_route(rawChunkId int, destinationsCosts *map[int]int) error {
	/*
		map operation over rawChunck resolved from his id
		chunk readed will be splitted in word and pre groupped by key using an hashmap (COMBINER function embedded)
		for the locallity aware routing of the next phase will be returned to master info about Mapper association to Reducer node
		in a dict of destNodeIde as key and token size.
		( will be decided Reducers positioning considering data locallity, also minimizing net Overhead)
	 */
	rawChunk:=getChunk(rawChunkId)					//TODO
	WorkerStateActual.workerStateInternal.intermediateTokens = make(map[string]int)
	*destinationsCosts=make(map[int]int)	//cost of routing to Reducers for intermediate tokens
	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	words := strings.FieldsFunc(rawChunk, f) //parse Go builtin by spaces
	//words:= strings.Fields(rawChunck)	//parse Go builtin by spaces
	for _, word := range words {
		WorkerStateActual.workerStateInternal.intermediateTokens[word]++
	}

	//reflect.Type.Size()
	//unsafe.Sizeof(word)
	//building reverse map for smart activations of ReducerNodes
	var destReducerNodeId int
	for k,v :=range WorkerStateActual.workerStateInternal.intermediateTokens{
		destReducerNodeId=hashKeyReducerSum(k,Configuration.WORKERNUMREDUCE)
		(*destinationsCosts)[destReducerNodeId]=estimateTokenSize(Token{k,v})
	}
	return nil
}
func (m *_map) Map_quick_route_reducers(reducersAddresses map[int]string) error {
	/*
		master will comunicate how to route intermediate tokens to reducers by this RPC
		reducersAddresses binds reducers ID to their actual address
		selected by the master minimizing the network overhead exploiting the data locallity on mapper nodes
	*/
	var destReducerNodeId int
	var destReducerNodeAddress string
	for k, v := range WorkerStateActual.workerStateInternal.intermediateTokens {
		destReducerNodeId = hashKeyReducerSum(k, Configuration.WORKERNUMREDUCE)
		destReducerNodeAddress = reducersAddresses[destReducerNodeId]

		//TODO REDUCE RPC CALLS HERE
	}
}

func estimateTokenSize(token Token) int {
	//return unsafe.Sizeof(token.V)+unsafe.Sizeof(token.K[0])*len(token.K)	//TODO CAST ERR
}



func getChunk(chunkID int) string {
	//TODO GET CHUNK FROM CHUNKS CACHE, EVENUTALLY FROM File ????
}

func (m *_map) Map_string_builtin(rawChunck string, tokens *map[string]int) error {	//TODO DEPRECATED
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



func (m *_map) Map_raw_parse(rawChunck string, tokens *map[string]int) error { //TODO DEPRECATED
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

////////// 		REDUCE 		///////////////
type _reduce int
type ReduceArg struct {
	//reduce argument for Reduction list of Values of a Key
	Key    string
	Values []int
}

func (r *_reduce) Reduce_IntermediateTokens(intermediateTokens map[string]int) error {
	WorkerStateActual.workerStateInternal.cumulativeCalls++ //update cummulative calls
	for k,v :=range intermediateTokens{
		//cumulate intermediate token values in a cumulative dictionary
		WorkerStateActual.workerStateInternal.intermediateTokensCumulative[k]+=v
	}
	if WorkerStateActual.workerStateInternal.cumulativeCalls==WorkerStateActual.workerStateExternal.expectedCalls{
		//TODO RETURN TO MASTER REDUCE OUTPUTS
		go returnReduce(WorkerStateActual.workerStateInternal.intermediateTokensCumulative)

	}
	return nil
}




func (r *_reduce) Reduce_tokens_key(args ReduceArg, outToken *Token) error { //TODO OLD VERSION
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

////////		MASTER		///////////////////////////////
type _master int
var FinalTokens []Token			//TODO LINK FROM GLOBAL ONE, TODO SMART PREALLOCATION

func (master *_master) returnReduce(finalTokensPartial map[string]int) error{
	for k,v:=range finalTokensPartial{
		FinalTokens= append(FinalTokens,Token{k,v})
	}
	return nil
}