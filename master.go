package main

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
)

///COSTANTS TODO CONFIG_FILE?go config file...
const (
	blockSize       = 20480 //byte size for each file block
	WorkerNumReduce = 6
)

var WorkerNumMap int //1 mapper per chunk of input
type worker struct {
	//struct for worker info
	busy bool
	addr string
}

func init_files_structs(file *os.File, chunksDest *TEXT_FILE, barrier *sync.WaitGroup) {
	//read filename in a struct TEXT_FILE separating text in chunks by pointer chunksDest
	*chunksDest = readFile(file)
	barrier.Done()
}

type RPCAsyncWrap struct { //wrap 1 rpc calls vars
	done    chan *rpc.Call
	divCall *rpc.Call
	Reply   Token
} //TODO USE IN MAP ASSIGN TOO LATER TESTS

func assignWorks_map(files_chunkized []TEXT_FILE) ([]map[string]int, error) {
	/*handle data chunk assignment to map worker
	only a chunk will be assigned to a map rpc server by async rpc
	rpc server are simulated in thread
	map termiate on all rpc request receved an answer
	*/

	///MAP RPC CALLs
	// Try to connect master to rpc map servers rysed  --rpc client init---
	//support vectors for rpc calls and replys
	clients := make([]*rpc.Client, WorkerNumMap)
	replys := make([]map[string]int, WorkerNumMap)
	divCalls := make([]*rpc.Call, WorkerNumMap)
	var err error
	//DIAL RPC SERVERs
	for x := 0; x < len(clients); x++ {
		//port := PORTBASE+x
		//address := fmt.Sprint("localhost:%d",port)		//OLDS
		_worker := Workers[x]
		clients[x], err = rpc.Dial("tcp", _worker.address)
		if err != nil {
			log.Println("Error in dialing: ", err)
			return nil, err //propagate on error
		}
		defer clients[x].Close()
	} //all map OPs correctly requested to map servers

	/// Asynchronous calls ... RPC REQs
	for x := 0; x < len(clients); x++ {
		divCalls[x] = clients[x].Go("Map.map_raw_parse", files_chunkized[x].blocks, &replys[x], nil)
	}
	//COLLECT RPC MAP RESULTS... TODO BARRIER OVERNEEDED	////
	outMapRes := make([]map[string]int, WorkerNumMap)
	//wait for rpc calls completed
	for z := 0; z < len(divCalls); z++ {
		divCall := <-divCalls[z].Done //block until rpc map call z has completed
		if divCall.Error != nil {
			log.Println("Error in map...: ", divCall.Error.Error())
			return nil, divCall.Error
		}
		outMapRes[z] = replys[z]
	}
	return outMapRes, nil
}

func assignWorks_Reduce(tokensMap map[string][]int) ([]Token, error) {
	//assign reduce works to Reduce workers
	//version with 1 rpc call per unique key
	//TODO ALTERNATIVE group reduce works by reducer id then send in group....overload
	clients := make([]*rpc.Client, WorkerNumReduce)
	workerCallCounters := [WorkerNumReduce]int{} //keep track of reduce call num per reduce worker ..to collect result
	//replys := make ([][]Token,WorkerNumReduce)
	//divCalls := make([]*rpc.Call,WorkerNumReduce)

	avgCallPerWorkerFair := len(tokensMap) / WorkerNumReduce
	rpcs_var_wrapped := make([][]RPCAsyncWrap, WorkerNumReduce) //rpc struct to handle all rpc calls
	var err error
	//rpc link init
	for x := 0; x < len(clients); x++ { //DIAL RPC REDUCE SERVERs
		_worker := Workers[x] //take references for destWorker worker for reduce op
		clients[x], err = rpc.Dial("tcp", _worker.address)
		if err != nil {
			log.Println("Error in dialing: ", err)
			return nil, err //propagate on error
		}
		rpcs_var_wrapped[x] = make([]RPCAsyncWrap, avgCallPerWorkerFair)
		workerCallCounters[x] = 0
	}
	//INVOKE RPC REDUCE CALLS...1 rpc call per key...
	for k, v := range tokensMap {
		destWorker := hashKeyReducerSum(k, WorkerNumReduce)
		arg := ReduceArg{key: k, values: v}
		//evalutate to extend rpc struct num
		callsNumOfWorker := workerCallCounters[destWorker] //ammount of rpc call done on destWorker
		if callsNumOfWorker > avgCallPerWorkerFair {       //EXTEND SPACE OF REPLY MATRIX ON NEED
			rpcs_var_wrapped[destWorker] = append(rpcs_var_wrapped[destWorker], RPCAsyncWrap{})
		}
		rpcWrap_k := &rpcs_var_wrapped[destWorker][callsNumOfWorker] //istance of struct for rpc of key k
		workerCallCounters[destWorker]++
		//replyRef:=&replys[destWorker][workerCallCounters[destWorker]] //get pntr of reply in replys matrix
		rpcWrap_k.divCall = clients[destWorker].Go("Reduce._reduce_tokens_key", arg, rpcWrap_k.Reply, nil)
	}
	outTokens := make([]Token, len(tokensMap)) //all result list
	var tokenCounter int = 0
	//////			COLLECT REDUCE RESULTs	////
	//iterate among rpcWrappers structures and append result in final Token list
	//block for each scheduled rpc , set result on outTokens using a counter
	for x := 0; x < len(rpcs_var_wrapped); x++ {
		for y := 0; y < len(rpcs_var_wrapped[x]); y++ {
			rpcWraped := &rpcs_var_wrapped[x][y]
			done := <-rpcWraped.done //block until rpc compleated
			if done.Error != nil {
				log.Println("Error in reduce...: ", done.Error.Error())
				return nil, done.Error
			}
			//append result of this terminated rpc
			outTokens[tokenCounter] = rpcWraped.Reply
		}
	}
	return outTokens, nil
}

//	///SHUFFLE & SORT	/////////
func mergeToken(tokenList []map[string]int) map[string][]int {
	//merge map results grouping by keys
	//return special map (key->values) for reduce works assigns
	outTokenGrouped := make(map[string][]int)
	//merge tokens produced by mappers
	for x := 0; x < len(tokenList); x++ {
		for k, v := range tokenList[x] {
			outTokenGrouped[k] = append(outTokenGrouped[k], v) //append values of dict_x to proper key group
		}
	} // out Token now contains all Token obtained from map works
	//// todo SORTING LIST ???? overneeded

	return outTokenGrouped
}

/*OLD
func mergeToken(tokenList []map[string]int) []Token {
	//merge map from map phase result return a list of Token
	//TODO UNCOMMENT FOR SORTING ...??? cardellini cara mia le slide !=!=!)!)!=!?!
	var middleListLen int = 0
	//achive Token list len by maps len...
	for x := 0; x < len(tokenList); x++ {
		middleListLen += len(tokenList[x])
	}
	outToken := make([]Token, middleListLen) //overstimed list len... TODO TOO MUCH -> MEMORY WASTE  OK ON MEM INTENSIVE APP

	c := 0 //counter to assign map keys to out Token list
	for x := 0; x < len(tokenList); x++ {
		//merge tokens produced by mappers
		for k, v := range tokenList[x] {
			outToken[c] = Token{k, v}
			c++
		}
	} // out Token now contains all Token obtained from map works

	//// SORTING LIST ..see https://golang.org/pkg/sort/
	//tks:=tokenSorter{outToken}
	//sort.Sort(tks)
	//fmt.Println(&tks.tokens,&outToken,&tks.tokens==&outToken) //TODO SORT IN PLACE...EFFECTED ON ORIGINAL LIST ?
	return outToken
}*/

var Workers []WORKER

func main() { //flexible main by := xD :) :D
	//TODO FILENAMES FROM os.args  slice..
	var filenames []string
	filesChunkized := make([]TEXT_FILE, len(filenames))
	////	INIT PHASE	/////
	//chunkize files
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	for i, filename := range filenames {
		f, err := os.Open(filename)
		check(err)
		WorkerNumMap += chunksAmmount(f) //set mapper ammount counting all chunks
		fmt.Println("reading ", filename)
		go init_files_structs(f, &filesChunkized[i], barrierRead) //read files in chunks by different threads
		e := f.Close()
		check(e)
	}
	/*initialize the max num of required workers for map and reduce phases
	will be initializated the max in worker required in map and reduce phase
	after map, overneeded workers will be terminated by chan
	*/
	Workers = workersInit(max(WorkerNumMap, WorkerNumReduce))
	barrierRead.Wait() //wait chunkization of files END

	///	MAP PHASE		/////////
	mapResoults, err := assignWorks_map(filesChunkized) //reqeust and collect MAP ops via rpc
	if err != nil {
		fmt.Println("ERROR", err)
		os.Exit(95)
	}
	for x := 0; x < max(0, WorkerNumMap-WorkerNumReduce); x++ { //terminate overneeded worker for reduce phase
		Workers[x].terminate <- true //terminate worker
	}

	///	SHUFFLE & SORT PHASE 	//////////////////
	tokenAll := mergeToken(mapResoults)

	///	REDUCE PHASE	/////////////////////
	defTokens, err := assignWorks_Reduce(tokenAll)
	if err != nil {
		log.Println(err)
		os.Exit(95)
	}
	fmt.Println("FINAL RESULT", defTokens)

	os.Exit(0)

}
