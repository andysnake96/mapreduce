package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
)

const (
	CONFIGFILENAME = "config.json"
	OUTFILENAME    = "finalTokens.txt"
)

var Configuration struct {
	WORKERNUMREDUCE int
	WORKERNUMMAP    int
	PORTBASE        int
}
var WORKERNUMREDUCE int = 20
var WORKERNUMMAP int = 30
var PORTBASE int = 2019

//)const (
//	//BLOCKSIZE       = 20480 //byte size for each file block	//TODO GOOGLE MAP REDUCE USE FIXED CHUNK SIZE
//	WORKERNUMREDUCE = 20
//	WORKERNUMMAP = 30
//	OUTFILENAME     = "finalTokens.txt"
//	PORTBASE        = 2019
//)

var Workers []WORKER //list of infos set of workers up & running
var OpenedFiles []*os.File

func assignWorks_map(chunks []CHUNK) []map[string]int {
	/*
		handle data chunk assignment to map worker
		only a chunk will be assigned to a map rpc server by async rpc
		rpc server are simulated in thread
		map termiate when all rpc requests have received an answer
	*/

	///MAP RPC CONNECTION
	// Try to connect master to rpc map servers previously  rysed
	//support vectors for rpc calls and replys
	//clients := make([]*rpc.Client, WorkerNumMap)
	replys := make([]map[string]int, WORKERNUMMAP)
	divCalls := make([]*rpc.Call, WORKERNUMMAP)
	/// ASYNC RPC CALLS TO WORKERS !!!!
	for x := 0; x < len(chunks); x++ {
		divCalls[x] = Workers[x].client.Go("Map.Map_parse_builtin", chunks[x], &replys[x], nil)
	}
	///	COLLECT RPC MAP RESULTS
	outMapRes := make([]map[string]int, WORKERNUMMAP)
	//wait for rpc calls completed
	for z := 0; z < len(divCalls); z++ {
		divCall := <-divCalls[z].Done //block until rpc map call z has completed
		check(divCall.Error)
		outMapRes[z] = replys[z]
	}
	return outMapRes
}

//	///SHUFFLE & SORT	/////////
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

	workerCallCounters := [WORKERNUMREDUCE]int{} //keep track of reduce call num per reduce worker ..to collect result
	var totKeys = len(tokensMap)
	fmt.Println("reducing Middle values #:=", totKeys)
	avgCallPerWorkerFair := len(tokensMap) / WORKERNUMREDUCE
	rpcs_var_wrapped := make([][]RPCAsyncWrap, WORKERNUMREDUCE) //rpc struct to handle all rpc calls on collection
	//////	rpc links setup
	for x := 0; x < WORKERNUMREDUCE; x++ {
		//TODO connection=client rdl?
		//_worker := Workers[x] //take references for destWorker worker for reduce op
		//clients[x], err = rpc.Dial("tcp", _worker.address)
		//if err != nil {
		//	log.Println("Error in dialing: ", err)
		//	return nil, err //propagate on error
		//}
		rpcs_var_wrapped[x] = make([]RPCAsyncWrap, avgCallPerWorkerFair)
		workerCallCounters[x] = 0
	}
	//reduce work assignement to workers can be done by hash function or RR policy
	var destWorker int //destination worker ID for reduction of values of a key
	var c int = 0      //counter for RR assignement

	////////	INVOKE RPC REDUCE CALLS...
	for k, v := range tokensMap {
		//compute destination worker ID
		//destWorker = hashKeyReducerSum(k, WORKERNUMREDUCE)		//hash of key assignement
		destWorker = c % int(WORKERNUMREDUCE)
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
			check(done.Error)
			//append result of this terminated rpc
			outTokens[tokenCounter] = rpcWraped.Reply
			tokenCounter++
		}
	}
	return outTokens, nil
}

func _init_chunks(filenames []string) []CHUNK {
	/*
		initialize chunk structure ready to be assigned to map workers
		files will be readed in multiple threads and totalsize will be divided in fair chunks sizes
		eventually the size of the reminder of division for assignment will be assigned to last chunk
	*/
	println("---INIT PHASE---")
	filesChunkized := make([]CHUNK, WORKERNUMMAP) //chunkset for assignement
	filesData := make([]string, len(filenames))
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	var totalWorkSize int64 = 0
	//////	chunkize files
	for i, filename := range filenames { //evaluting total work size for fair assignement
		f, err := os.Open(filename)
		check(err)
		OpenedFiles[i] = f
		go func(barrierRead **sync.WaitGroup, destData *string) { //read all files in separated threads
			allbytes, err := ioutil.ReadAll(bufio.NewReader(f))
			check(err)
			*destData = string(allbytes)
			(*barrierRead).Done()
			runtime.Goexit()
		}(&barrierRead, &filesData[i])
		fstat, err := f.Stat()
		check(err)
		totalWorkSize += fstat.Size()
	}
	chunkSize := int64(totalWorkSize / WORKERNUMMAP) //avg like chunk size
	reminder := int64(totalWorkSize % WORKERNUMMAP)  //assigned to first worker
	barrierRead.Wait()                               //wait read data end in all threads
	allStr := strings.Join(filesData, "")

	var low, high int64
	for x := 0; x < len(filesChunkized); x++ {
		low = chunkSize * int64(x)
		high = chunkSize * int64(x+1)
		filesChunkized[x] = CHUNK(allStr[low:high])
	}
	if reminder > 0 {
		filesChunkized[len(filesChunkized)-1] = CHUNK(allStr[low : high+reminder]) //last worker get bigger chunk
	}
	return filesChunkized

}

func main() { // main wrapper, used for test call
	//var filenames []string = os.Args[1:]
	var filenames []string = []string{"/home/andysnake/Scrivania/uni/magistrale/DS/src/mapReduce/txtSrc/1012-0.txt"}
	//filenames := []string{"/home/andysnake/Scrivania/books4GoPrj/oscarWilde/dorianGray.txt", "/home/andysnake/Scrivania/books4GoPrj/oscarWilde/soulOfAMen.txt"}
	defTokens := _main(filenames) //MAP&REDUCE HERE
	serializeToFile(defTokens, OUTFILENAME)
	os.Exit(0)
}

func _main(filenames []string) []Token {
	//main payload , return final processed tokens ready to be serialized

	if len(filenames) == 0 {
		fmt.Fprint(os.Stderr, "USAGE <plainText1, plainText2, .... >\n")
	}
	OpenedFiles = make([]*os.File, len(filenames))
	chunks := _init_chunks(filenames)

	/*initialize the max num of required workers for map and reduce phases*/
	_workerNum := max(WORKERNUMMAP, WORKERNUMREDUCE)
	Workers = workersInit(_workerNum)

	//cleanUp files opened before
	cleanUpFiles(OpenedFiles)

	fmt.Println("SUCCESSFULLY RYSED: ", _workerNum, " workers!")
	///		MAP PHASE		/////////
	println("---		MAP PHASE		---")
	mapResoults := assignWorks_map(chunks) //reqeust and collect MAP ops via rpc
	//terminate workers thread overneeded after map phase
	if WORKERNUMMAP > WORKERNUMREDUCE {
		for x := 0; x < max(0, WORKERNUMMAP-WORKERNUMREDUCE); x++ {
			Workers[_workerNum-1-x].terminate <- true //terminate worker x using a bool chan
		}
		Workers = Workers[:WORKERNUMREDUCE]
	}

	///	SHUFFLE & SORT PHASE 	//////////////////
	println("---		SHUFFLE & SORT PHASE		---")
	tokenAll := mergeToken(mapResoults)

	///	REDUCE PHASE	/////////////////////
	println("---		REDUCE PHASE		---")
	defTokens, err := assignWorks_Reduce(tokenAll)
	if err != nil {
		log.Println(err)
		os.Exit(95)
	}

	for _, worker := range Workers { //terminate residue worker thread when reduce phases has compleated
		worker.terminate <- true
	}
	return defTokens

}
