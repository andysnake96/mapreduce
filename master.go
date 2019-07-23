package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sort"
)


const (
	CONFIGFILENAME = "config.json"
	OUTFILENAME    = "finalTokens.txt"
)

var Configuration struct {
	WORKERNUMREDUCE int
	WORKERNUMMAP    int
	SORT            bool			//sort final file (extra computation)
}
var Workers []Worker //list of infos set of workers up & running
var OpenedFiles []*os.File


func main() { // main wrapper, used for test call
	var filenames []string = []string{"/home/andysnake/GolandProjects/mapreducego/txtSrc/1012-0.txt"}
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}
	ReadConfigFile()
	defTokens := _main(filenames) //MAP&REDUCE HERE
	if Configuration.SORT {
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

	/*initialize the max num of required workers for map and reduce phases*/
	_workerNum := max(Configuration.WORKERNUMMAP, Configuration.WORKERNUMREDUCE)
	Workers = workersInit(_workerNum)

	//cleanUp files opened before
	cleanUpFiles(OpenedFiles)

	fmt.Println("SUCCESSFULLY RYSED: ", _workerNum, " workers!")
	///		MAP PHASE		/////////
	fmt.Println("---		MAP PHASE		---")
	mapResoults := assignWorks_map(chunks) //reqeust and collect MAP ops via rpc
	//TODO OLD terminate workers thread overneeded after map phase
	//if Configuration.WORKERNUMMAP > Configuration.WORKERNUMREDUCE {
	//	for x := 0; x < max(0, Configuration.WORKERNUMMAP-Configuration.WORKERNUMREDUCE); x++ {
	//		Workers[_workerNum-1-x].terminate <- true //terminate Worker x using a bool chan
	//	}
	//	Workers = Workers[:Configuration.WORKERNUMREDUCE]
	//}

	///	SHUFFLE & SORT PHASE 	//////////////////
	fmt.Println("---		SHUFFLE & SORT PHASE		---")
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

}


///////////////////// OLD VERSION FOR REFERENCE	//////////////////
func assignWorks_map(chunks []CHUNK) []map[string]int {
	/*
		handle data chunk assignment to map Worker
		only a chunk will be assigned to a map rpc server by async rpc
		rpc server are simulated in thread
		map termiate when all rpc requests have received an answer
	*/

	///MAP RPC CONNECTION
	// Try to connect master to rpc map servers previously  rysed
	//support vectors for rpc calls and replys

	replys := make([]map[string]int, Configuration.WORKERNUMMAP)
	divCalls := make([]*rpc.Call, Configuration.WORKERNUMMAP)
	/// ASYNC RPC CALLS TO WORKERS !!!!
	for x := 0; x < len(chunks); x++ {
		divCalls[x] = Workers[x].client.Go("Map.Map_parse_builtin", chunks[x], &replys[x], nil)
	}
	///	COLLECT RPC MAP RESULTS
	outMapRes := make([]map[string]int, Configuration.WORKERNUMMAP)
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

type RPCAsyncWrap struct { //wrap 1 rpc calls variables for reduce phase
	divCall *rpc.Call
	Reply   Token
}

func assignWorks_Reduce(tokensMap map[string][]int) ([]Token, error) {
	//assign reduce works to Reduce workers,collecting result will be built final token list

	workerCallCounters := make([]int, Configuration.WORKERNUMREDUCE) //keep track of reduce call num per reduce Worker ..to collect result

	var totKeys = len(tokensMap)
	fmt.Println("reducing Middle values #:=", totKeys)
	avgCallPerWorkerFair := len(tokensMap) / Configuration.WORKERNUMREDUCE
	rpcs_var_wrapped := make([][]RPCAsyncWrap, Configuration.WORKERNUMREDUCE) //rpc struct to handle all rpc calls on collection
	//////	rpc links setup
	for x := 0; x < Configuration.WORKERNUMREDUCE; x++ {
		rpcs_var_wrapped[x] = make([]RPCAsyncWrap, avgCallPerWorkerFair)
		workerCallCounters[x] = 0
	}
	//reduce work assignement to workers can be done by hash function or RR policy
	var destWorker int //destination Worker ID for reduction of values of a key
	var c int = 0      //counter for RR assignement

	////////	INVOKE RPC REDUCE CALLS...
	for k, v := range tokensMap {
		//compute destination Worker ID
		//destWorker = hashKeyReducerSum(k, Configuration.WORKERNUMREDUCE)		//hash of key assignement
		destWorker = c % int(Configuration.WORKERNUMREDUCE)
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
	outTokens := make([]Token, len(tokensMap)) //all result list
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
