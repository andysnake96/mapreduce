package mapReduce

import (
	"fmt"
	"os"
)

///COSTANTS TODO CONFIG_FILE?go config file...
const (
	blockSize = 20480 //byte size for each file block
)

type worker struct {
	//struct for worker info
	busy bool
	addr string
}

var WorkerNum int       //ammount of map worker usable
var WorkerNumMap int    //ammount of map worker usable
var WorkerNumReduce int //ammount of map worker usable
func init_files_structs(filenames []string, done chan []TEXT_FILE) {
	//read filenames in TEXT_FILE structures returned..
	numFiles := len(filenames)
	out := make([]TEXT_FILE, numFiles)
	for x := 0; x < len(filenames); x++ {
		filename := filenames[x]
		out[x] = readFile(filename)

	}
	done <- out
}

func assignWork_map(files_chunkized []TEXT_FILE) []map[string]int {
	//handle data chunk assignment to map worker
	//return map works outputs in a list ...
	//TODO handle reassignment on worker fault pinging periodically
	out := make([]map[string]int, WorkerNumMap)

	return out //TODO NIL ON FATAL ERROR
}
func assignWork_Reduce(mergedTokens []token) {
	//assign token to reduce workers using hashing keys
	tokenSetsReduce := make([][]token, WorkerNumReduce) //list of set for each reduce worker
	tokenForReducer := len(mergedTokens) / WorkerNumReduce
	//init keys for reduce worker in a matrix of slice of token
	for x := 0; x < WorkerNumReduce; x++ {
		tokenSetsReduce = append(tokenSetsReduce, make([]token, tokenForReducer))
	}
	//assign token to reduce worker sets fast hashing on token's key
	for x := 0; x < len(mergedTokens); x++ {
		//destination subset for key of index x
		destSubSet := hashKeyReducerSum(mergedTokens[x].key, WorkerNumReduce)
		tokenSetsReduce[destSubSet] = append(tokenSetsReduce[destSubSet], mergedTokens[x])
		//TODO HASH PER MAPPARE KEYS->REDUCE WORKER (ID=int)
		//TODO ITERATING AMONG TOKENS GENERETE SUBSETs FOR REDUCE WORKER
	}
	//TODO RPC TO REDUCE WORKER i WITH ARG tokenSetsReduce[i] tokens

	/*
		sigolo thread:
		 ? leggi & raggruppa --> CHE SENSO reduce dopo !=?HA WTF
		-->!!!!volendo leva sort e dopo merge di dizionari passa direttamente alla generazione dei subset...TUTTO HA SENSO !===!???
		IDEA PARALLELIZZANTE: modo migliore di farlo concorrentemente...
		---opzione1:
		1 thread per ogni subset di token4Reduce a cui assegno una porzione di mergedTokens (MOLTO GRANDE)
		dato che a tale thread è assegnato solo 1! subset
			LUI SCRIVERA SOLO CHIAVI CHE SI MAPPANO IN TALE SUBSET E GLI VERRANO CUMUNICATE ALTRE CHIAVI CHE SI MAPPANO LI TRAMITE CHANNEL
		quindi ogni thread ha un suo channel da cui legge alla fine dello scorrimento dell listone chiavi che si mappano da altri thread ...

	*/
	//TODO RETURN RPC CALLS VALUE AS A DEFINITIVE MAP LIST OF keywd-> value
}
func main() {
	var filenames []string
	//TODO FILENAMES FROM os.args  slice..
	var filesChunkized []TEXT_FILE //block of files in filenames

	////	INIT	/////
	filesReadChan := make(chan []TEXT_FILE)
	go init_files_structs(filenames, filesReadChan)
	initWorkersChan := make(chan int)
	//go init_workers(configs)

	//wait init phases to terminate, checking channels
	for x := 0; x < 2; x++ {
		select {
		case filesChunkized = <-filesReadChan:
			fmt.Println("chunkized file...")
		case <-initWorkersChan:
			fmt.Println("Worker initialized and ready :)")
		}
	}
	///MAP PHASE
	mapResoults := assignWork_map(filesChunkized)
	if mapResoults == nil {
		fmt.Println("ERROR IN MAP WORKS ASSIGN ...")
	}
	///SHUFFLE & SORT PHASE
	tokenSorted := mergeToken(mapResoults)
	///REDUCE PHASE
	assignWork_Reduce(tokenSorted)
	///COLLECT

	os.Exit(0)

}

func mergeToken(tokenList []map[string]int) []token {
	//merge map from map phase result return a list of token
	//TODO UNCOMMENT FOR SORTING ...??? cardellini cara mia le slide !=!=!)!)!=!?!
	var middleListLen int = 0
	//achive token list len by maps len...
	for x := 0; x < len(tokenList); x++ {
		middleListLen += len(tokenList[x])
	}
	outToken := make([]token, middleListLen) //overstimed list len... TODO TOO MUCH -> MEMORY WASTE  OK ON MEM INTENSIVE APP

	c := 0 //counter to assign map keys to out token list
	for x := 0; x < len(tokenList); x++ {
		//merge tokens produced by mappers
		for k, v := range tokenList[x] {
			outToken[c] = token{k, v}
			c++
		}
	} // out token now contains all token obtained from map works

	//// SORTING LIST ..see https://golang.org/pkg/sort/
	//tks:=tokenSorter{outToken}
	//sort.Sort(tks)
	//fmt.Println(&tks.tokens,&outToken,&tks.tokens==&outToken) //TODO SORT IN PLACE...EFFECTED ON ORIGINAL LIST ?
	return outToken
}
