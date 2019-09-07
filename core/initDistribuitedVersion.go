package core

import (
	"../aws_SDK_wrap"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"net"
	"net/rpc"
	"os"
	"strconv"
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

	if loadChunksToS3 { //avoid usless aws put waste if chunks are already loaded to S3
		//init chunks loading to storage service
		println("loading chunks of file to S3")
		go loadChunksToChunkStorage(filenames, &barrier, &control, uploader)
	} else {
		barrier.Add(-1)
	}

	////// sync worker init routines
	barrier.Wait()
	println("initialization done")
	return downloader, uploader, assignedPorts
}

func loadChunksToChunkStorage(filenames []string, waitGroup **sync.WaitGroup, control **MASTER_CONTROL, uploader *s3manager.Uploader) {
	//chunkize filenames and upload to storage service

	chunks := InitChunks(filenames) //chunkize filenames
	//initialize upload stuff
	uploadAllBarrier := new(sync.WaitGroup)
	uploadAllBarrier.Add(len(chunks))
	bucket := Config.S3_BUCKET
	chunkIDS := make([]int, len(chunks))
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
		chunkIDS[i] = i
	}
	uploadAllBarrier.Wait()
	stopTime := time.Now()
	(*control).ChunkIDS = chunkIDS
	(*waitGroup).Done()
	println("loaded: ", len(chunks), " approx in : ", stopTime.Sub(startTime).String())
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
	masterRegServiceAddr := (*control).Addresses.Master + ":" + strconv.Itoa(port)
	err = aws_SDK_wrap.UploadDATA(uploader, masterRegServiceAddr, MASTER_ADDRESS_PUBLISH_S3_KEY, Config.S3_BUCKET)
	if CheckErr(err, false, "") {
		return err
	}
	workerAddresses := WorkerAddresses{
		WorkersMapReduce:  make([]string, Config.WORKER_NUM_MAP),
		WorkersOnlyReduce: make([]string, Config.WORKER_NUM_ONLY_REDUCE),
		WorkersBackup:     make([]string, Config.WORKER_NUM_BACKUP_WORKER),
	}

	//// aggreagate worker registration with 2! for
	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
	}
	var destWorkersContainer []string //will hold destination container for  worker
	for workerKind, numToInit := range workersKindsNums {
		println("initiating: ", numToInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == WORKERS_MAP_REDUCE {
			destWorkersContainer = workerAddresses.WorkersMapReduce
		} else if workerKind == WORKERS_ONLY_REDUCE {
			destWorkersContainer = workerAddresses.WorkersOnlyReduce
		} else if workerKind == WORKERS_BACKUP_W {
			destWorkersContainer = workerAddresses.WorkersBackup
		}
		for i := 0; i < numToInit; i++ {
			//WAIT WORKERS TO REGISTER TO COMUNICATED MASTER ADDRESS EXTRACTING ADDRESS FROM PROBE SYNs
			workerConn, err := conn.AcceptTCP()
			if CheckErr(err, false, "worker connection error") {
				return err
			}
			destWorkersContainer[i] = workerConn.RemoteAddr().String() //set registered address
			_ = workerConn.Close()
		}
	}
	(*control).Addresses = workerAddresses
	(*waitGroup).Done()
	return nil
}

////////// WORKER SIDE

func InitWorker(worker *Worker_node_internal, stopPingChan chan bool) ([]int, error) {
	////// start ping service and initialize worker
	assignedPorts := make([]int, 0, 5)
	/// init worker struct
	pingPort := NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &assignedPorts, true, true, "udp")        //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
	controlRpcPort := NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &assignedPorts, true, true, "tcp") //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS

	pingConn, err := PingHeartBitRcv(pingPort, stopPingChan)
	if CheckErr(err, false, "worker init distribuited version") {
		return assignedPorts, nil
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
	}

	err, _ = InitRPCWorkerIstance(nil, controlRpcPort, CONTROL, worker)
	if CheckErr(err, false, "control rpc init on worker failed") {
		return assignedPorts, err
	}
	return assignedPorts, nil
}

func RegisterToMaster(downloader *s3manager.Downloader) (string, error) {
	//get master published address from s3
	//register as new worker to master

	//get master address
	masterAddress := make([]byte, len("255.255.255.255:9696"))
	for v := 0; v < len(masterAddress); v++ { //GO memset (xD)
		masterAddress[v] = 0
	}
	err := aws_SDK_wrap.DownloadDATA(downloader, Config.S3_BUCKET, MASTER_ADDRESS_PUBLISH_S3_KEY, masterAddress, false)
	if CheckErr(err, false, "master addr fetch err") {
		return "", err
	}
	masterAddressStr := string(masterAddress)
	i := 0
	for i = len(masterAddress) - 1; i >= 0; i-- { //eliminate residue part from downloaded string
		if masterAddress[i] != 0 { //enough to go back until !0
			break
		}
	}
	masterAddressStr = masterAddressStr[:i+1]
	println("fetched master address for my registration: ", masterAddressStr)

	//register to master
	conn, err := net.Dial("tcp", masterAddressStr)
	if CheckErr(err, true, "dial error") {
		return "", err
	}
	masterAddressStr = conn.RemoteAddr().String()
	return masterAddressStr, nil
}

func MasterAddressFetch() string {

	return ""
}
