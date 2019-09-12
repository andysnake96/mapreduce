package core

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/text/encoding/unicode"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

//////////// FLEX CONFIGURATION
type ConfigInterface interface {
	printFields()
}
type Configuration struct {
	SORT_FINAL                 bool //sort final file (extra computation)
	LOCAL_VERSION              bool //use function for local deply
	SIMULATE_WORKERS_CRUSH     bool
	SIMULATE_WORKERS_CRUSH_NUM int
	ISTANCES_NUM_REDUCE        int    //number of reducer to istantiate
	WORKER_NUM_ONLY_REDUCE     int    //num of worker node that will exec only 1 reduce istance
	WORKER_NUM_MAP             int    //num of mapper to istantiate
	WORKER_NUM_BACKUP_WORKER   int    //num of backup workers for crushed workers
	BACKUP_MASTER              bool   //num of backup masters
	RPC_TYPE                   string //tcp or http
	// main rpc services base port (other istances on same worker will have progressive port
	CHUNK_SERVICE_BASE_PORT           int
	MAP_SERVICE_BASE_PORT             int
	REDUCE_SERVICE_BASE_PORT          int
	MASTER_BASE_PORT                  int
	PING_SERVICE_BASE_PORT            int
	FIXED_PORT                        bool
	WORKER_REGISTER_SERVICE_BASE_PORT int
	// main loadBalacing vars
	MAX_REDUCERS_PER_WORKER int
	// main replication vars
	CHUNKS_REPLICATION_FACTOR                int
	CHUNKS_REPLICATION_FACTOR_BACKUP_WORKERS int
	CHUNK_SIZE                               int64
	// AWS
	LoadChunksToS3 bool
	S3_REGION      string
	S3_BUCKET      string
	FAIL_RETRY     int
}

func (config *Configuration) printFields() {
	ReflectionFieldsGet(config)
}

//support variable for dinamic generated addresses read
type WorkerAddresses struct {
	//refs to up&running workers
	WorkersMapReduce  []string
	WorkersOnlyReduce []string
	WorkersBackup     []string
	Master            string
}

func (addrs *WorkerAddresses) printFields() {
	ReflectionFieldsGet(addrs)
}

//shared configuration
var Config *Configuration

var Addresses *WorkerAddresses //global configuration
const (
	CONFIGFILENAME         = "configurations/config.json"
	ADDRESSES_GEN_FILENAME = "configurations/addresses.json"
	OUTFILENAME            = "finalTokens.txt"
)

var FILENAMES_LOCL = []string{"txtSrc/1012-0.txt"}

//errors constant for fault revery
const ( //errors kinds
	REDUCER_ACTIVATE           = "REDUCER_ACTIVATE"           //reducer activation error -->id reducer (logic)
	REDUCERS_ADDR_COMUNICATION = "REDUCERS_ADDR_COMUNICATION" //reducer collocation comunication to worker --> id of worker with mappers
	REDUCE_CONNECTION          = "REDUCE_CONNECTION"          //worker connection reducer error	--> reduce id logic
	REDUCE_CALL                = "REDUCE_CALL"                //reduce() error					--> reduce id logic
	ERROR_SEPARATOR            = " "                          //errors sub field separator
	TIMEOUT                    = "TIMEOUT"                    // rpc timeout

)

func ParseReduceErrString(reduceRpcErrs []error, data *MASTER_STATE_DATA, moreWorkerFails map[int]bool) (map[int][]int, map[int][]int) {
	//parse reduce rpc error string, find failed worker setting map/reduce JOB to reset

	mapsToRedo := make(map[int][]int)
	reduceToRedo := make(map[int][]int)
	failedWorkers := make([]int, 0, len(reduceRpcErrs))
	for _, err := range reduceRpcErrs {
		tmpErrString := strings.Split(err.Error(), ERROR_SEPARATOR) //key value in 2 string FAIL_TYPE-->ID OF FAILED
		failedId, _ := strconv.Atoi(tmpErrString[1])
		workerFailedID := 0
		if tmpErrString[0] == REDUCERS_ADDR_COMUNICATION { //worker fail during bindings comunication
			workerFailedID = failedId
		} else {
			workerFailedID = data.ReducerSmartBindingsToWorkersID[failedId] //worker hosting failed reduce
		}
		failedWorkers = append(failedWorkers, workerFailedID)
	}
	for _, workerFailedID := range failedWorkers {
		lostMapJobs, doesExist := data.AssignedChunkWorkersFairShare[workerFailedID]
		if doesExist {
			mapsToRedo[workerFailedID] = lostMapJobs
		}
		for reducerID, hostWorker := range data.ReducerSmartBindingsToWorkersID {
			if workerFailedID == hostWorker {
				reduceToRedo[workerFailedID] = append(reduceToRedo[workerFailedID], reducerID)
			}
		}
	}
	//use ping aliveness filter to get know of others failed workers, witch error has not been propagated e.g. failed reducer over failed mapper
	for workerFailedID, _ := range moreWorkerFails {
		//skip if already treated this worker
		_, alreadyKnowFailM := mapsToRedo[workerFailedID]
		_, alreadyKnowFailR := reduceToRedo[workerFailedID]
		if alreadyKnowFailM || alreadyKnowFailR {
			continue
		}
		lostMaps, doesExist := data.AssignedChunkWorkersFairShare[workerFailedID]
		if doesExist {
			mapsToRedo[workerFailedID] = lostMaps
		}
		for reducerID, hostWorker := range data.ReducerSmartBindingsToWorkersID {
			if workerFailedID == hostWorker {
				reduceToRedo[workerFailedID] = append(reduceToRedo[workerFailedID], reducerID)
			}
		}
	}
	return mapsToRedo, reduceToRedo
}

type Token struct {
	//rappresent Token middle V out from map phase
	K string
	V int //Key occurence on prj 1
}

func CleanUpFiles(files []*os.File) {
	for _, f := range files {
		e := f.Close()
		CheckErr(e, true, "")
	}
}

func InitChunks(filenames []string) []CHUNK { //fast concurrent file read for chunk generation
	/*
		initialize chunk structure ready to be assigned to map workers
		files will be readed in multiple threads and totalsize will be divided in fair chunks sizes
		eventually the size of the reminder of division for assignment will be assigned to last chunk
	*/
	openedFiles := make([]*os.File, len(filenames))
	fmt.Println("---start chunkization---")
	filesData := make([]string, len(filenames))
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	var totalWorkSize int64 = 0
	//////	chunkize files
	for i, filename := range filenames { //evaluting total work size for fair assignement
		f, err := os.Open(filename)
		CheckErr(err, true, "")
		openedFiles[i] = f
		go func(barrierRead **sync.WaitGroup, destData *string) { //read all files in separated threads
			allbytes, err := ioutil.ReadAll(bufio.NewReader(f))
			CheckErr(err, true, "")
			*destData = string(allbytes)
			(*barrierRead).Done()
			runtime.Goexit()
		}(&barrierRead, &filesData[i])
		fstat, err := f.Stat()
		CheckErr(err, true, "")
		totalWorkSize += fstat.Size()
	}
	barrierRead.Wait() //wait read data end in all threads
	defer closeFileLists(openedFiles)

	var chunkSize int64
	var reminder int64
	var numChunk int
	fixedChunkSize := Config.CHUNK_SIZE > 0
	// chunk num and size based on configuration
	if fixedChunkSize { //fixed chunk size
		chunkSize = Config.CHUNK_SIZE
		numChunk = int(totalWorkSize / chunkSize)
		reminder = totalWorkSize % chunkSize
		if reminder > 0 {
			numChunk++
		}
	} else { //fixed chunk num
		numChunk = Config.WORKER_NUM_MAP
		chunkSize = int64(totalWorkSize / int64(numChunk)) //avg like chunk size
		reminder = int64(totalWorkSize % int64(numChunk))  //assigned to first Worker
	}
	filesChunkized := make([]CHUNK, numChunk)
	allStr := strings.Join(filesData, "")

	var low, high int64
	for x := 0; x < len(filesChunkized); x++ {
		low = chunkSize * int64(x)
		high = Min(chunkSize*int64(x+1), int64(len(allStr))) //fixed chunk size and positive reminder => last extra chunk smaller
		filesChunkized[x] = CHUNK(allStr[low:high])
	}
	if !fixedChunkSize && reminder > 0 {
		filesChunkized[len(filesChunkized)-1] = CHUNK(allStr[low : high+reminder]) //last Worker get bigger chunk
	}
	return filesChunkized
}

func closeFileLists(files []*os.File) {
	for _, file := range files {
		err := file.Close()
		CheckErr(err, true, "closing files stage...")
	}
}

//// HASHING KEY FUNCs
func HashKeyReducerSum(key string, maxIDOut int) int {
	//simply hash string to int in [0,maxIDOut) by sum chars and %
	//for the given key string will return the ID of dest reducer
	const EXTRASHUFFLE = 96 //extra shuffle in hash func
	sum := 0
	for c := range key {
		sum += c
	}
	sum += EXTRASHUFFLE
	return sum % maxIDOut
}

//// SORT_FINAL SUPPORT FUNCTION
// FOR TOKEN LIST
type TokenSorter struct {
	//rappresent Token middle V out from map phase
	Tokens []Token
	//by func(tks1,tks2 Token)	//sorting function ... default by Key builtin sort func
}

func (t TokenSorter) Len() int {
	return len(t.Tokens)
}
func (t TokenSorter) Swap(i, j int) {
	t.Tokens[i], t.Tokens[j] = t.Tokens[j], t.Tokens[i]
}
func (t TokenSorter) Less(i, j int) bool {
	return t.Tokens[i].V < t.Tokens[j].V
}

////  ROUTING COSTS
type RoutingCostsSorter struct {
	routingCosts []TrafficCostRecord
}

func (r RoutingCostsSorter) Len() int {
	return len(r.routingCosts)
}
func (r RoutingCostsSorter) Swap(i, j int) {
	r.routingCosts[i], r.routingCosts[j] = r.routingCosts[j], r.routingCosts[i]
}
func (r RoutingCostsSorter) Less(i, j int) bool {
	return r.routingCosts[i].RoutingCost < r.routingCosts[j].RoutingCost
}

func GetEndianess() unicode.Endianness {
	var i int = 0x0100
	ptr := unsafe.Pointer(&i)
	if 0x01 == *(*byte)(ptr) {
		fmt.Println("Big Endian")
		return unicode.BigEndian
	} else {
		fmt.Println("")
		return unicode.LittleEndian
	}

}

/////	HEARTBIT 	/////
const PING_LEN = 4

const (
	PING = iota
	PONG
	CHUNK_ASSIGN
	MAP_ASSIGN
	LOCALITY_AWARE_LINK_REDUCE
	ENDED
)
const PING_TRY_NUM = 5

var PING_TIMEOUT time.Duration = time.Millisecond * 960

func PingHeartBitRcvMaster(port int, stateChan chan uint32) (net.Conn, error) {
	//ping receve and reply service under port implemented with ping/pong of 1 byte readed/written by a routine
	//stopPing has to be a initiated and 1 buffered channel for non blocking read
	//return the listen udp connection to caller or error
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if CheckErr(err, false, "ping rcv error") {
		return conn, err
	}
	message := make([]byte, PING_LEN)
	masterDead := false
	var state uint32
	/// go ping reaceiver rountine
	go func() {
		for masterDead {
			_, remoteAddr, err := conn.ReadFromUDP(message) //PING
			if CheckErr(err, false, "udp read err") {
				break
			}
			/// read state if changed and append to next pong message
			if len(stateChan) > 0 {
				state = <-stateChan //unblocking chan read
			}
			binary.BigEndian.PutUint32(message, state)
			_, err = conn.WriteToUDP(message, remoteAddr) //PONG
			if CheckErr(err, false, "udp write err") {
				break
			}
		}
	}()
	_ = conn.Close()
	return nil, nil
}

func PingHeartBitRcv(port int, stopPing chan bool) (net.Conn, error) {
	//ping receve and reply service under port implemented with ping/pong of 1 byte readed/written by a routine
	//stopPing has to be a initiated and 1 buffered channel for non blocking read
	//return the listen udp connection to caller or error

	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if CheckErr(err, false, "ping rcv error") {
		return conn, err
	}
	/// go ping reaceiver rountine
	go func() {
		message := make([]byte, PING_LEN)
		pong := make([]byte, PING_LEN)
		pong[0] = PONG
		for len(stopPing) < 1 {
			//ping read&reply loop until stop has been set on bufferd channel in stopPing
			//TODO STOP SERVICE DURING READ BLOCK => ERROR PRINT
			//error check fatal setted to false on stop service propagated to ping routie
			_, remoteAddr, err := conn.ReadFromUDP(message) //PING
			if CheckErr(err, false, "udp read err") {
				break
			}
			_, err = conn.WriteToUDP(pong, remoteAddr) //PONG
			if CheckErr(err, false, "udp write err") {
				break
			}
			//data := strings.TrimSpace(string(message[:receivedLen]))
			//fmt.Printf("received: %s from %s\n", data, remoteAddr)
		}
		_ = conn.Close()
	}()
	return conn, nil
}

func PingHeartBitSnd(addr string) (error, uint32) {
	//ping host at addr with 1byte ping msg waiting for a pongBuf or a timeout expire
	//fixed num of trys performed

	/// setup destination
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	CheckErr(err, true, "udp Addr resolve connection error")
	udpConn, err := net.DialUDP("udp", nil, udpAddr) //nil laddr will set a euphimeral port on sender
	defer udpConn.Close()
	CheckErr(err, true, "udp dial error")

	///// set up ephemeral source socket
	//euphimeralAddr:=udpConn.LocalAddr().String();
	//myIp:=strings.Split(euphimeralAddr,":")[0];
	//ephemeralPortStr :=strings.Split(euphimeralAddr,":")[1];
	//ephemeralPort,err:=strconv.Atoi(ephemeralPortStr);
	//CheckErr(err,true,"ephemeral port extracting error");
	//ephemeralConn, err := net.ListenUDP("udp", &net.UDPAddr{
	//	 Port: ephemeralPort,
	//	 IP:   net.ParseIP(myIp),
	// })
	//CheckErr(err,true,"ephemeral conn listen error");
	//defer ephemeralConn.Close();

	pongBuf := make([]byte, PING_LEN)
	var pong uint32
	ping := make([]byte, PING_LEN)
	binary.BigEndian.PutUint32(ping, PING)
	myEndianess := GetEndianess()
	socketFail := true
	err = udpConn.SetReadDeadline(time.Now().Add(PING_TIMEOUT))
	CheckErr(err, true, "set deadline to ping socket error")
	/// fixed num of ping try
	for i := 0; i < PING_TRY_NUM && socketFail; i++ {
		//// write ping
		_, err = udpConn.Write(ping) //PING
		CheckErr(err, true, "udp write ping error")
		//// read pongBuf converting from netw byte order to host byte order
		_, err = udpConn.Read(pongBuf) //PONG rcv
		if CheckErr(err, false, "") {
			socketFail = true //no error exit ping try loop
			continue
		}
		//// convert received pong to host endianess
		if myEndianess == unicode.LittleEndian {
			pong = binary.LittleEndian.Uint32(pongBuf)
		} else {
			pong = binary.BigEndian.Uint32(pongBuf)
		}
		socketFail = false
	}
	if socketFail {
		return err, 0
	}
	return nil, pong
}

func PingProbeAlivenessFilter(control *MASTER_CONTROL) map[int]bool {
	workers := &(control.Workers)
	failedWorkers := make(map[int]bool, len(control.WorkersAll))
	//probe each worker for aliveness, filter away dead workers
	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: len(workers.WorkersMapReduce), WORKERS_ONLY_REDUCE: len(workers.WorkersOnlyReduce), WORKERS_BACKUP_W: len(workers.WorkersBackup),
	}
	var destWorkersContainer *[]Worker //dest variable for workers to init
	var err error
	for workerKind, _ := range workersKindsNums {
		if workerKind == WORKERS_MAP_REDUCE {
			destWorkersContainer = &(workers.WorkersMapReduce)
		} else if workerKind == WORKERS_ONLY_REDUCE {
			destWorkersContainer = &(workers.WorkersOnlyReduce)
		} else if workerKind == WORKERS_BACKUP_W {
			destWorkersContainer = &(workers.WorkersBackup)
		}
		//taking worker kind addresses list
		workersNotFailed := make([]Worker, 0, len(*destWorkersContainer))
		for i := 0; i < len(*destWorkersContainer); i++ {
			worker := (*destWorkersContainer)[i]

			if worker.State.Failed { //avoid useless ping probe
				err = errors.New("failed worker")
				failedWorkers[worker.Id] = true
			} else {
				err, _ = PingHeartBitSnd(worker.Address + ":" + strconv.Itoa(worker.PingServicePort))
				if err != nil {
					failedWorkers[worker.Id] = true
				}
			}
			if err == nil {
				workersNotFailed = append(workersNotFailed, worker)
			}
		}
		*destWorkersContainer = workersNotFailed
	}
	(*control).WorkersAll = append((*workers).WorkersMapReduce, (*workers).WorkersOnlyReduce...)
	(*control).WorkersAll = append((*control).WorkersAll, (*workers).WorkersBackup...)
	return failedWorkers
}

////	INSTANCES FUNCs
func GetMaxIdWorkerInstances(workerInstances *map[int]WorkerInstanceInternal) int {
	maxId := 0
	for id, _ := range *workerInstances {
		if id > maxId {
			maxId = id //updateMaxId
		}
	}
	return maxId
}
func GetMaxIdWorkerInstancesGenericDict(workerInstancesMap interface{}) int {
	v := reflect.ValueOf(workerInstancesMap)
	if v.Kind() != reflect.Map {
		panic("TRYING TO GET KEYS FROM A NON MAP but have" + v.Kind().String())
	}
	ids := v.MapKeys()
	maxId := 0
	for _, id := range ids {
		if int(id.Int()) > maxId {
			maxId = int(id.Int()) //updateMaxId
		}
	}
	//GenericPrint(ids)
	//println(maxId)
	return maxId
}

/// OTHER
func CheckErrs(errs []error, fatal bool, supplementMsg string) bool {

	for _, e := range errs {
		if CheckErr(e, fatal, supplementMsg) == true {
			return true
		}
	}
	return false
}
func CheckErr(e error, fatal bool, supplementMsg string) bool {
	//check error, exit if fatal is true
	//return bool, true if err is not nil
	if e != nil {
		//baseMsg := e.Error()		//noted that errors always pre print before return to caller
		baseMsg := ""
		if fatal == true {
			log.Fatal("\n\n"+supplementMsg, e)
		} else {
			log.Println("\n\n"+baseMsg+supplementMsg, e)
		}
		return true
	}
	return false
}
func Max(a int64, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
func Min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

/////////// (DE) Serializaiton
func SerializeToFile(defTokens []Token, filename string) {
	/////	SERIALIZE RESULT TO FILE
	n := 0
	lw := 0
	encodeFile, err := os.Create(filename)
	CheckErr(err, true, "")
	_, err = encodeFile.Seek(0, 0)
	CheckErr(err, true, "")
	defer encodeFile.Close()
	for _, tk := range defTokens {
		line := fmt.Sprint(tk.K, "->", tk.V, "\r\n")
	write:
		n, err = encodeFile.WriteString(line[lw:])
		CheckErr(err, true, "")
		if n < len(line) {
			lw += n
			fmt.Println("write short...")
			goto write
		}
		lw = 0
	}
}

// go binary encoder
func SerializeMasterStateBase64(m MASTER_CONTROL) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	if err != nil {
		fmt.Println(`failed gob Encode`, err)
	}
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

// go binary decoder
func DeSerializeMasterStateBase64(str string) MASTER_CONTROL {
	out := MASTER_CONTROL{}
	by, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		fmt.Println(`failed base64 Decode`, err)
	}
	b := bytes.Buffer{}
	b.Write(by)
	d := gob.NewDecoder(&b)
	err = d.Decode(&out)
	if err != nil {
		fmt.Println(`failed gob Decode`, err)
	}
	return out
}
func ListOfDictCumulativeSize(dictList []map[int]int) int {
	cumulativeSum := 0
	for _, dict := range dictList {
		cumulativeSum += len(dict)
	}
	return cumulativeSum
}
func DictsNestedCumulativeSize(dictsNestes map[int]map[int]int) int {
	cumulativeSum := 0
	for _, dict := range dictsNestes {
		cumulativeSum += len(dict)
	}
	return cumulativeSum
}
func CheckAllTrueInBoolDict(boolDict map[int]bool) bool {
	for _, value := range boolDict {
		if value == false {
			return false
		}
	}
	return true
}
func ReadConfigFile(configFilePath string, destVar ConfigInterface) {
	f, err := os.Open(configFilePath)
	CheckErr(err, true, "config file open")
	defer f.Close()
	//configRawStr,err:=ioutil.ReadAll(bufio.NewReader(f))
	decoder := json.NewDecoder(f)
	err = decoder.Decode(destVar)
	CheckErr(err, true, "")

}

func ReflectionFieldsGet(strct interface{}) {
	val := reflect.ValueOf(strct)
	values := make(map[string]interface{}, val.NumField())
	metaTypes := val.Type()
	for i := 0; i < val.NumField(); i++ {
		values[metaTypes.Field(i).Name] = val.Field(i).Interface()
	}

	fmt.Println(values)

}

//func GenericPrint(slice interface{}, prefixMsg string) bool {
//	println(prefixMsg)
//	sv := reflect.ValueOf(slice)
//	for i := 0; i < sv.Len(); i++ {
//		fmt.Printf("	\t%d\t", sv.Index(i).Interface())
//	}
//	fmt.Printf("\n\n")
//	return false
//}
func GenericPrint(slice []int, prefixMsg string) bool {

	for _, value := range slice {
		prefixMsg += "\t" + strconv.Itoa(value)
	}
	println(prefixMsg)
	return false
}

//ports
func CheckPortAvaibility(port int, network string) (status bool) {

	// Concatenate a colon and the port
	errs := make([]error, 2)
	// Try to create a server with the port
	if network == "tcp" {
		server, err := net.Listen(network, ":"+strconv.Itoa(port))
		errs = append(errs, err)
		if err == nil {
			err = server.Close()
		}
		errs = append(errs, err)

	} else {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{
			Port: port,
			IP:   net.ParseIP("0.0.0.0"),
		})
		errs = append(errs, err)
		if err == nil {
			err = conn.Close()
		}
		errs = append(errs, err)
	}

	return !CheckErrs(errs, false, "")

}

func RandomBool(probability float64, digitsNumSignificativance int) bool {
	//return true with probability

	rand.Seed(time.Now().UnixNano())
	maxN := int(math.Pow10(digitsNumSignificativance))
	trueThreashold := int(math.Round(probability * float64(maxN)))
	v := rand.Intn(maxN)
	if v < trueThreashold {
		return true
	} else {
		return false
	}

}

func SlicesEQ(ints1 []int, ints2 []int) bool {
	if len(ints1) != len(ints2) {
		return false
	}
	for i, value := range ints1 {
		if ints2[i] != value {
			return false
		}
	}
	return true
}
func MapsEq(map1 map[int]string, map2 map[int]string) bool {
	if len(map1) != len(map2) {
		return false
	}
	for key, value := range map1 {
		val, present := map2[key]
		if !present || val != value {
			return false
		}
	}
	return true
}
