package core

import (
	"../aws_SDK_wrap"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MASTER_ADDRESS_PUBLISH_S3_KEY = "MASTER_ADDRESS"

//////////MASTER SIDE
type MASTER_CONTROL struct {
	MasterRpc  *MasterRpc
	Workers    WorkersKinds //connected workers
	WorkersAll []Worker     //list of all workers ref.
	Addresses  WorkerAddresses
	ChunkIDS   []int //list of all avaibles  chunks
}

func Init_distribuited_version(control *MASTER_CONTROL, filenames []string, loadChunksToS3 bool) (*s3manager.Downloader, *s3manager.Uploader, []int) {
	///initialize data and workers referement

	barrier := new(sync.WaitGroup)
	barrier.Add(2)
	downloader, uploader := aws_SDK_wrap.InitS3Links(Config.S3_REGION)
	assignedPorts := make([]int, 0, 10)
	//init workers,letting them register to master, he will populate different workers kind in ordered manner
	go func() {
		err := waitWorkersRegister(&barrier, control, &assignedPorts, uploader)
		CheckErr(err, true, "workers initialization failed :(")
	}()

	chunks := InitChunks(filenames) //chunkize filenames
	if loadChunksToS3 {             //avoid usless aws put waste if chunks are already loaded to S3
		//init chunks loading to storage service
		println("loading chunks of file to S3")
		go loadChunksToChunkStorage(chunks, &barrier, control, uploader)
	} else {
		(*control).ChunkIDS = BuildSequentialIDsListUpTo(len(chunks))
		barrier.Add(-1)
	}

	////// sync worker init routines
	barrier.Wait()
	println("initialization done")
	return downloader, uploader, assignedPorts
}

func loadChunksToChunkStorage(chunks []CHUNK, waitGroup **sync.WaitGroup, control *MASTER_CONTROL, uploader *s3manager.Uploader) {
	//chunkize filenames and upload to storage service

	//initialize upload stuff
	uploadAllBarrier := new(sync.WaitGroup)
	uploadAllBarrier.Add(len(chunks))
	bucket := Config.S3_BUCKET
	chunkIDS := BuildSequentialIDsListUpTo(len(chunks))
	errs := make([]error, 0)
	errsMutex := sync.Mutex{}
	println("concurrent S3 UPLOAD OF ", len(chunks), " CHUNKS start")
	startTime := time.Now()
	for i, _ := range chunks {
		keyChunk := strconv.Itoa(i)
		go func(barrier **sync.WaitGroup, i int) {
			err := aws_SDK_wrap.UploadDATA(uploader, string(chunks[i]), keyChunk, bucket)
			if err != nil {
				_, _ = fmt.Fprint(os.Stderr, "upload err", err)
				errsMutex.Lock()
				errs = append(errs, err)
				errsMutex.Unlock()
			}
			print(".")
			(*barrier).Done()
		}(&uploadAllBarrier, i)
	}
	uploadAllBarrier.Wait()
	stopTime := time.Now()
	(*control).ChunkIDS = chunkIDS
	(*waitGroup).Done()
	println("loaded: ", len(chunks), " approx in : ", stopTime.Sub(startTime).String())
}

func BuildSequentialIDsListUpTo(maxID int) []int {
	list := make([]int, maxID)
	for i := 0; i < maxID; i++ {
		list[i] = i
	}
	return list
}

func waitWorkersRegister(waitGroup **sync.WaitGroup, control *MASTER_CONTROL, assignedPorts *[]int, uploader *s3manager.Uploader) error {
	//setup worker register service tcp port at master
	//publish this address to workers
	//wait workers to registry to master

	port := NextUnassignedPort(Config.WORKER_REGISTER_SERVICE_BASE_PORT, assignedPorts, true, true, "tcp")
	conn, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: port,
		IP:   net.ParseIP("0.0.0.0"),
	})
	if CheckErr(err, false, "REGISTER SERVICE SOCKET FAIL") {
		return err
	}
	///////publish master address to workers
	println("uploading master registration addresse: ", conn.Addr().String())
	if (*control).Addresses.Master != "" {
		masterRegServiceAddr := (*control).Addresses.Master + ":" + strconv.Itoa(port)
		err = aws_SDK_wrap.UploadDATA(uploader, masterRegServiceAddr, MASTER_ADDRESS_PUBLISH_S3_KEY, Config.S3_BUCKET)
		if CheckErr(err, false, "") {
			return err
		}
	}
	workerAddresses := WorkerAddresses{
		WorkersMapReduce:  make([]string, Config.WORKER_NUM_MAP),
		WorkersOnlyReduce: make([]string, Config.WORKER_NUM_ONLY_REDUCE),
		WorkersBackup:     make([]string, Config.WORKER_NUM_BACKUP_WORKER),
	}
	workers := WorkersKinds{
		WorkersMapReduce:  make([]Worker, Config.WORKER_NUM_MAP),
		WorkersOnlyReduce: make([]Worker, Config.WORKER_NUM_ONLY_REDUCE),
		WorkersBackup:     make([]Worker, Config.WORKER_NUM_BACKUP_WORKER),
	}
	//// aggreagate worker registration with 2! for
	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
	}
	var destWorkersAddrList []string  //will hold destination container for  worker
	var destWorkersContainer []Worker //dest variable for workers to init
	id := 0                           //worker id
	for workerKind, numToInit := range workersKindsNums {
		println("initiating: ", numToInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == WORKERS_MAP_REDUCE {
			destWorkersAddrList = workerAddresses.WorkersMapReduce
			destWorkersContainer = (workers.WorkersMapReduce)
		} else if workerKind == WORKERS_ONLY_REDUCE {
			destWorkersAddrList = workerAddresses.WorkersOnlyReduce
			destWorkersContainer = (workers.WorkersOnlyReduce)
		} else if workerKind == WORKERS_BACKUP_W {
			destWorkersAddrList = workerAddresses.WorkersBackup
			destWorkersContainer = (workers.WorkersBackup)
		}
		for i := 0; i < numToInit; i++ {
			//WAIT WORKERS TO REGISTER TO COMUNICATED MASTER ADDRESS EXTRACTING ADDRESS FROM PROBE SYNs
			workerConn, err := conn.AcceptTCP()
			if CheckErr(err, false, "worker connection error") {
				return err
			}
			workerAddr := strings.Split(workerConn.LocalAddr().String(), ":")[0]
			destWorkersAddrList[i] = workerAddr
			portPing := Config.PING_SERVICE_BASE_PORT
			portControlRpc := Config.CHUNK_SERVICE_BASE_PORT
			if !Config.FIXED_PORT { ///TODO
				portBuf := make([]byte, len("6666;7777"))
				readed := 0
				rd := 0
				lastReaded := ""
				for readed < len(portBuf) && err != io.EOF && lastReaded != PORT_TERMINATOR {
					rd, err = workerConn.Read(portBuf[readed:])
					if err != io.EOF && CheckErr(err, false, "WORKER INIT ERROR") {
						return err
					}
					readed += rd
					lastReaded = string(portBuf[readed-1])
				}
				ports := strings.Split(string(portBuf), PORT_SEPARATOR)
				portControlRpc, _ = strconv.Atoi(ports[0])
				portPing, _ = strconv.Atoi(ports[1])
			}
			//// init workers connections
			println("initiating worker with controPort", portControlRpc, "ping port ", portPing)
			client, err := rpc.Dial(Config.RPC_TYPE, workerAddr+":"+strconv.Itoa(portControlRpc))
			CheckErr(err, true, "dialing connected worker errd")
			destWorkersContainer[i] = Worker{
				Address:         workerAddr,
				PingServicePort: portPing,
				Id:              id,
				State: WorkerStateMasterControl{
					ChunksIDs: make([]int, 0, 5),
					ControlRPCInstance: WorkerIstanceControl{
						Port:   portControlRpc,
						Kind:   CONTROL,
						Client: client,
					},
					Failed: false,
				},
			}
			_ = workerConn.Close()
			id++
		}
	}
	(*control).Addresses = workerAddresses
	//build all worker ref
	workersAll := append(workers.WorkersMapReduce, workers.WorkersOnlyReduce...)
	workersAll = append(workersAll, workers.WorkersBackup...)
	(*control).Workers = workers
	(*control).WorkersAll = workersAll

	(*waitGroup).Done()
	return nil
}

////////// WORKER SIDE
const PORT_SEPARATOR = ";"  // for flexible port assignement worker will comunicate during registration CONTROL_RPC_PORT;PING_SERVICE_PORT
const PORT_TERMINATOR = "-" // for flexible port assignement worker will comunicate during registration CONTROL_RPC_PORT;PING_SERVICE_PORT

func InitWorker(worker *Worker_node_internal, stopPingChan chan bool, downloader *s3manager.Downloader) ([]int, error) {
	////// start ping service and initialize worker
	assignedPorts := make([]int, 0, 5)
	/// init worker struct
	pingPort := NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &assignedPorts, true, true, "udp")        //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
	controlRpcPort := NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &assignedPorts, true, true, "tcp") //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS

	_, _ = fmt.Fprint(os.Stderr, controlRpcPort, pingPort)
	pingConn, err := PingHeartBitRcv(pingPort, stopPingChan)
	if CheckErr(err, false, "worker init distribuited version error") {
		return assignedPorts, err
	}
	*worker = Worker_node_internal{
		WorkerChunksStore: WorkerChunks{
			Mutex:  sync.Mutex{},
			Chunks: make(map[int]CHUNK),
		},
		IntermediateDataAggregated: AggregatedIntermediateTokens{
			ChunksSouces:                 make([]int, 0, 5),
			PerReducerIntermediateTokens: make([]map[string]int, Config.ISTANCES_NUM_REDUCE),
		},
		Instances: make(map[int]WorkerInstanceInternal),

		ReducersClients: make(map[int]*rpc.Client),
		ExitChan:        make(chan bool),
		PingConnection:  pingConn,
		PingPort:        pingPort,
		Downloader:      downloader,
	}

	err, _ = InitRPCWorkerIstance(nil, controlRpcPort, CONTROL, worker)
	if CheckErr(err, false, "control rpc init on worker failed") {
		return assignedPorts, err
	}
	return assignedPorts, nil
}

const masterADDR = "37.116.178.139:6000"

func RegisterToMaster(downloader *s3manager.Downloader, portsComunication string) (string, error) {
	//get master published address from s3
	//register as new worker to master
	//if setted flexible port assignement for worker servieces (control rpc instance and ping service) comunicate to master portsComunication

	//get master address
	//masterAddress := make([]byte, len("255.255.255.255:9696"))
	//for v := 0; v < len(masterAddress); v++ { //GO memset (xD)
	//	masterAddress[v] = 0
	//}
	//err := aws_SDK_wrap.DownloadDATA(downloader, Config.S3_BUCKET, MASTER_ADDRESS_PUBLISH_S3_KEY, masterAddress, false)
	//if CheckErr(err, false, "master addr fetch err") {
	//	return "", err
	//}
	//masterAddressStr := string(masterAddress)
	//i := 0
	//for i = len(masterAddress) - 1; i >= 0; i-- { //eliminate residue part from downloaded string
	//	if masterAddress[i] != 0 { //enough to go back until !0
	//		break
	//	}
	//}
	//masterAddressStr = masterAddressStr[:i+1]
	masterAddressStr := masterADDR //TODO TEMP SAVE S3 GET limit free tier
	println("fetched master address for my registration: ", masterAddressStr)

	//register to master
	conn, err := net.Dial("tcp", masterAddressStr)
	if CheckErr(err, true, "dial error") {
		return "", err
	}
	defer conn.Close()
	if !Config.FIXED_PORT {
		_, err = conn.Write([]byte(portsComunication)) //err not nil if wrt < len
		if CheckErr(err, false, "\n\n\n\n\n\ncomunicating ping port to master") {
			return "", err
		}
	}
	masterAddressStr = strings.Split(conn.RemoteAddr().String(), ":")[0]
	println("registered to master", portsComunication)
	return masterAddressStr, nil
}

func MasterAddressFetch() string {

	return ""
}
