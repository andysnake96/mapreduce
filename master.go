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
	ADDRESSES_GEN_FILENAME = "config.json"
	OUTFILENAME    = "finalTokens.txt"
)
type ConfigInterface interface {
	printFields()
}
type Configuration struct {
	SORT_FINAL               bool //sort final file (extra computation)
	LOCAL_VERSION            bool //use function for local deply
	WORKER_NUM_ONLY_REDUCE   int  //num of worker node that will exec only reduce istances
	ISTANCES_NUM_REDUCE      int  //number of reducer to istantiate
	WORKER_NUM_MAP           int  //num of mapper to istantiate
	WORKER_NUM_BACKUP_WORKER int  //num of backup workers for crushed workers
	WORKER_NUM_BACKUP_MASTER int  //num of backup masters
	RPC_TYPE                 string	//tcp or http
	// main rpc services base port (other istances on same worker will have progressive port
	CHUNK_SERVICE_BASE_PORT  int
	MAP_SERVICE_BASE_PORT    int
	REDUCE_SERVICE_BASE_PORT int
}
func (config *Configuration) printFields(){
	reflectionFieldsGet(config)
}
//support variable for dinamic generated addresses read
type WorkerAddresses struct{				//refs to up&running workers
	workersMapReduce 	[]string
	workersOnlyReduce 	[]string
	workersBackup 		[]string
}
func (addrs *WorkerAddresses) printFields(){
	reflectionFieldsGet(addrs)
}

const(
	////fault tollerant  enhancer
	CHUNK_SERVICE_ISTANCE_INDEX				=0	 //chunk service istance index in worker istances owned
	SEPARATED_REDUCERS_NUM                   = 2 //NUM of separated reducers to obtain in assigned
	WORKER_REPLICA_BACKUP_NUM                = 1 //idle worker with preassigned chunks
	CHUNKS_REPLICATION_FACTOR                =3  //replication factor of chunks assignement for each worker
	CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS =5  //replication factor for backup worker
)


//// MASTER CONTROL VARS

var Workers WorkersKinds
var ChunkIDS []int
var AssignedChunkWorkers map[*Worker][]int			//assigned chunk IDs to a worker
var OpenedFiles []*os.File
var Config Configuration		//global configuration
var Addresses WorkerAddresses	//global configuration

func main() {
	ReadConfigFile(CONFIGFILENAME,&Config)
	ReadConfigFile(ADDRESSES_GEN_FILENAME,&Addresses)
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
			//TODO HOT REPLICATION (1,2 EXTRA WORKER NODES ISTANCIATED)
		 */

	if Config.LOCAL_VERSION {
		init_local_version()
	}/*else{

	}*/
	/////	assign chunks
	/*
		fair distribuition of chunks among worker nodes with replication factor
			(will be assigned a fair share of chunks to each worker + a replication factor of chunks)
						(chunks replications R.R. of not already assigned chunks)
	*/
	assignChunksIDs(&Workers.workersMapReduce,&ChunkIDS,CHUNKS_REPLICATION_FACTOR,false)
	assignChunksIDs(&Workers.workersBackup,&ChunkIDS,CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS,true)	//only replication assignement on backup workers
	comunicateChunksAssignementToWorkers()							//RPC 1 IN SEQ DIAGRAM
	////	MAP
	var reducerTrafficCosts ReducersTrafficCosts
	reducerTrafficCosts = assignMapWorks()							//RPC 2,3 IN SEQ DIAGRAM
	reducerSmartBindings:=reducersBindingsLocallityAwareEuristic(reducerTrafficCosts)
	println(reducerSmartBindings)
	////	DATA LOCALITY AWARE REDUCER BINDINGS
	//activateReducers(reducerSmartBindings)												//RPC 4 IN SEQ DIAGRAM
	//comunicateReducersBindings(reducerSmartBindings)									//RPC 5 IN SEQ DIAGRAM;
	////	REDUCE																		// will be triggered in 6
	waitJobsEnd()																		//wait reduce END
	cleanUpResidueWorkers()												//close mappers not closed yet for fault tollerant

}
func init_local_version() {
	////// init files
	//TODO better script chunks S3 load
	var filenames []string = []string{"/home/andysnake/GolandProjects/mapreducego/txtSrc/1012-0.txt"}
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}

	////// load chunk to storage service
	ChunkIDS=loadChunksStorageService_localMock(filenames)
	////// init workers
	Workers=initWorkers_localMock_dinamic()		//TODO BOTO3 SCRIPT CONCURRENT STARTUP
}

func waitJobsEnd() {
	/*
	block main thread until REDUCERS workers will comunicate that all REDUCE jobs are ended
	 */
	//TODO channel lock wait, UNLOCKED ON |R| reduceMasterCallback received on master
}

func cleanUpResidueWorkers() {
	/*
	kill mapper not ended because of possibility resend info to restarted reducer (if eventually failed)
	 */
}

func assignMapWorks() ReducersTrafficCosts {
	return *new(ReducersTrafficCosts)
}

func comunicateChunksAssignementToWorkers() {
	ends:=make([]*rpc.Call,len(AssignedChunkWorkers))
	i:=0
	for workerPntr,chunksIds:=range AssignedChunkWorkers{
		ends[i]=(*workerPntr).state.chunkServIstance.client.Go("CHUNK.Get_chunk_ids",chunksIds,nil,nil)
		i++
	}
	for _,doneChan:=range ends{				//wait all assignment compleated
		<-doneChan.Done
	}
}
func assignChunksIDs(workers *[]Worker,chunksIds *[]int,replicationFactor int,onlyReplication bool) {
	/*
		fair share of chunks assigned to each worker plus a replication factor of chunks
		only latter if onlyReplication is true
	*/
	fairChunkNumShare:=int(len(*chunksIds)/len(*workers))
	fairChunkNumRemider:=int(len(*chunksIds)%len(*workers))
	var chunkIDsFairShareReminder []int						//reminder of fair chunk assignment
	if !onlyReplication {	//evaluate both fair assignement and replication
		chunkIDsFairShareReminder=(*chunksIds)[len(*chunksIds)-fairChunkNumRemider:len(*chunksIds)]

		//fair share chunk assignement
		workerInAssignementIndex:=0
		for j:=0;j<len(*chunksIds);j+=fairChunkNumShare{
			chunkIDsFairShare:=(*chunksIds)[j:j+fairChunkNumShare]
			worker:=(*workers)[workerInAssignementIndex]
			workerChunks:=&(worker.state.chunksIDs)
			*workerChunks=append((*workerChunks),chunkIDsFairShare...)				//TODO CHECK DEBUG
			AssignedChunkWorkers[&worker]=append(AssignedChunkWorkers[&worker],chunkIDsFairShare...)	//quicklink for smart replication
			workerInAssignementIndex++
		}

	}
	//CHUNKS REPLICATION and fair share remider assignement to all workers
	for _,worker:=range *workers{
		//reminder assignment
		worker.state.chunksIDs=append(worker.state.chunksIDs,chunkIDsFairShareReminder...)	// will append an empty list if OnlyReplciation is true//TODO CHECK DEBUG
		AssignedChunkWorkers[&worker]=append(AssignedChunkWorkers[&worker],chunkIDsFairShareReminder...)	//quick link for smart replication
		//replication assignment
		chunksReplicationToAssignID,err:=getChunksNotAlreadyAssignedRR(chunksIds,replicationFactor-fairChunkNumRemider,AssignedChunkWorkers[&worker])
		checkErr(err,false,"")
		worker.state.chunksIDs=append(worker.state.chunksIDs,chunksReplicationToAssignID...)

	}
}






func _old_main_wrapper() { //TODO OLD
	var filenames []string = []string{"/home/andysnake/GolandProjects/mapreducego/txtSrc/1012-0.txt"}
	//var filenames []string = os.Args[1:]
	if len(filenames) == 0 {
		log.Fatal("USAGE <plainText1, plainText2, .... >")
	}
	ReadConfigFile()
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

	/*initialize the max num of required workers for map and reduce phases*/
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

type RPCAsyncWrap struct { //wrap 1 rpc calls variables for reduce phase
	divCall *rpc.Call
	Reply   Token
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
