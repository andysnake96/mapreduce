package core

import (
	"errors"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
)

var AssignedPortsAll []int //list of assigned ports (globally) for local version

func NextUnassignedPort(basePort int, assignedPorts *[]int, assignNewPort bool, checkExternalPortBindings bool) int {
	//find next avaible port among the assignedPorts starting search from basePort,
	//will be returned the closest next assignable port
	//the new port will be assigned if true assignNewFoundedPort
	sort.Ints(*assignedPorts)
	conflict := false
	var nextPortUnassigned int
	port := basePort
	var portIndex int
	//check if base port is assigned
	for portIndex, nextPortUnassigned = range *assignedPorts {
		if nextPortUnassigned == port {
			port++ //first try to avoid port collision
			conflict = true
			break
		}
	}
	if conflict && portIndex+1 < len(*assignedPorts) { //on port assignement conflict=>find next avaible port starting from that location
		//also skip port collision sub resolve if nextPortUnassigned is already the last one
		portIndex++ //start search next port checking next assigned port
		for lastAssignedPort := (*assignedPorts)[len(*assignedPorts)-1]; nextPortUnassigned < lastAssignedPort; portIndex++ {
			nextPortUnassigned = (*assignedPorts)[portIndex]
			if nextPortUnassigned != port { //find port avaibility near to the baseport
				if !checkExternalPortBindings {
					goto foundedPort
				}
				for ; port < nextPortUnassigned && !CheckPortAvaibility(port); port++ { //check port binding to other app
					println("check if already bounded port:", port)
				}
				if CheckPortAvaibility(port) {
					goto foundedPort
				} else {
					continue //exceeded port during search, skip increment because already done in check port bounded by other apps
				}
			}
			port++
		}

		if !conflict {
			port++ //ended assigned port vector checks without find an avaible port==> just take the next
		}
	}
foundedPort:
	//println("founded avaible port ", port)
	if assignNewPort { //evalue to append port to appended port list
		*assignedPorts = append(*assignedPorts, port)
	}
	return port
}

func InitWorkers_LocalMock_WorkerSide(workers *[]Worker_node_internal,stopPingChan chan bool) {
	//local worker init version
	//workerSide version
	//worker initialized on localhost as routine with instances running on them (logically as other routine) with unique assigned ports for each rpc instance

	var avaiblePort int
	totalWorkerNum := Config.WORKER_NUM_MAP + Config.WORKER_NUM_BACKUP_WORKER + Config.WORKER_NUM_ONLY_REDUCE
	*workers = make([]Worker_node_internal, totalWorkerNum)
	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
	}

	workerId := 0
	AssignedPortsAll = make([]int, 0, totalWorkerNum)
	//initalize workers by kinds specified in configuration file starting with the chunk service instance
	for workerKind, numToInit := range workersKindsNums {
		println("initiating workers of type: ", workerKind)
		for i := 0; i < numToInit; i++ {
			workerNode := Worker_node_internal{
				WorkerChunksStore: WorkerChunks{
					Mutex:  sync.Mutex{},
					Chunks: make(map[int]CHUNK, WORKER_CHUNKS_INITSIZE_DFLT),
				},
				Instances:       make(map[int]WorkerInstanceInternal),
				ReducersClients: make(map[int]*rpc.Client),
				Id:              workerId,
				ExitChan:        make(chan bool),
				StartChan:       make(chan bool,1),
			}
			//starting worker control rpc instance
			avaiblePort = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
			e, _ := InitRPCWorkerIstance(nil, avaiblePort, CONTROL, &workerNode)
			CheckErr(e, true, "instantiating base instanc error...")
			/////////// ping service start
			avaiblePort = NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &AssignedPortsAll, true, true) //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
			conn,err:=PingHeartBitRcv(avaiblePort,stopPingChan)
			CheckErr(err,true,"heart bit init error")
			workerNode.PingConnection =conn

			// evaluating special fields basing on worker type
			if workerKind == WORKERS_MAP_REDUCE {
				workerNode.IntermediateDataAggregated = AggregatedIntermediateTokens{
					ChunksSouces:                 make([]int, 0),
					PerReducerIntermediateTokens: make([]map[string]int, Config.ISTANCES_NUM_REDUCE),
				}
				workerNode.ReducersClients = make(map[int]*rpc.Client, Config.WORKER_NUM_ONLY_REDUCE)
			} //else if .....
			println("started worker: ", workerId)
			(*workers)[workerId]=workerNode
			workerId++
		}
	}
	println(AssignedPortsAll)

}

func InitWorkers_LocalMock_MasterSide() (WorkersKinds, []Worker) {
	//init all kind of workers local version
	//master side version

	var worker Worker
	var port int
	totalWorkersNum := Config.WORKER_NUM_MAP + Config.WORKER_NUM_ONLY_REDUCE + Config.WORKER_NUM_BACKUP_WORKER
	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
	}
	//generic destination variable for init
	var addressesWorkers []string
	var destWorkersContainer *[]Worker //dest variable for workers to init
	//init out variables
	workersAllsRef := make([]Worker, 0, totalWorkersNum) //refs to all created workers (for deallocation master side)
	workersOut := *new(WorkersKinds)                      //actual workers to return
	workersOut.WorkersMapReduce = make([]Worker, len(Addresses.WorkersMapReduce))
	workersOut.WorkersOnlyReduce = make([]Worker, len(Addresses.WorkersOnlyReduce))
	workersOut.WorkersBackup = make([]Worker, len(Addresses.WorkersBackup))
	idWorker := 0
	for workerKind, numToInit := range workersKindsNums {
		println("initiating: ", numToInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == WORKERS_MAP_REDUCE {
			addressesWorkers = Addresses.WorkersMapReduce
			destWorkersContainer = &workersOut.WorkersMapReduce
		} else if workerKind == WORKERS_ONLY_REDUCE {
			addressesWorkers = Addresses.WorkersOnlyReduce
			destWorkersContainer = &workersOut.WorkersOnlyReduce
		} else if workerKind == WORKERS_BACKUP_W {
			addressesWorkers = Addresses.WorkersBackup
			destWorkersContainer = &workersOut.WorkersBackup
		}
		for i, address := range addressesWorkers {
			port = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
			pingPort:=NextUnassignedPort(Config.PING_SERVICE_BASE_PORT,&AssignedPortsAll,true,false)
			client, err := rpc.Dial(Config.RPC_TYPE, worker.Address+":"+strconv.Itoa(port))
			CheckErr(err, true, "init worker client")
			//init control rpc instance
			worker = Worker{
				Address:         address,
				PingServicePort: pingPort,
				Id:              idWorker,
				State: WorkerStateMasterControl{
					ChunksIDs:       make([]int, 0),
					WorkerNodeLinks: &worker,
					ControlRPCInstance:WorkerIstanceControl{
						Port:     port,
						Kind:     CONTROL,
						Client:   client,
					}}}

			//setting refs
			//noinspection ALL ... workers holders initiated before for
			(*destWorkersContainer)[i] = worker
			idWorker++
			println("initiated worker: ", worker.Id)
		}
	}
	workersAllsRef=append(workersOut.WorkersBackup,workersOut.WorkersMapReduce ...)
	workersAllsRef=append(workersAllsRef,workersOut.WorkersOnlyReduce...)
	return workersOut, workersAllsRef
}

func GetChunksNotAlreadyAssignedRR(chunksIds *[]int, numChunkToFind int, chunksIdsAssignedAlready []int) ([]int, error) {
	/*
			find first numChunkToFind not already present in list chunkAssignedAlready (not already assigned to worker

		//TODO SMART REPLICATION->sort global assignement Desc for redundancy num(placed chunk replicas)->select first numChunkToFind low replicated chunks to assign
	*/
	chunksToAssign := make([]int, 0)
	for i := 0; i < len(*chunksIds) && len(chunksToAssign) < numChunkToFind; i++ {
		chunkId := (*chunksIds)[i]
		alreadyAssignedChunk := false
		for _, chunkIdsAssignedAlready := range chunksIdsAssignedAlready {
			if chunkId == chunkIdsAssignedAlready {
				alreadyAssignedChunk = true
			}
		}
		if !alreadyAssignedChunk {
			chunksToAssign = append(chunksToAssign, chunkId)
		}
	}
	if len(chunksToAssign) < numChunkToFind {
		return chunksToAssign, errors.New("not finded all replication chunk to assign")

	}
	return chunksToAssign, nil
}

///// STORAGE SERVICE MOCK
var ChunksStorageMock map[int]CHUNK

func LoadChunksStorageService_localMock(filenames []string) []int {
	/*
		simulate chunk distribuited storage service in a local
		chunks will be generated and a map of ChunkID->chunk_data will be created
	*/
	chunks := InitChunks(filenames)
	chunkIDS := make([]int, len(chunks))
	ChunksStorageMock = make(map[int]CHUNK, len(chunks))
	for i, chunk := range chunks {
		chunkIDS[i] = i
		ChunksStorageMock[i] = chunk
	}
	return chunkIDS

}
func LoadChunks_multipleTimes_debug(filenames []string,replicationFactor int) []int {
	/*
		simulate chunk distribuited storage service in a local
		chunks will be generated and a map of ChunkID->chunk_data will be created
		each chunk will be replicated replciaitonFactor number of times for debugging check
	*/
	chunks := InitChunks(filenames)
	chunkIDS := make([]int, len(chunks)*replicationFactor)
	ChunksStorageMock = make(map[int]CHUNK, len(chunks)*replicationFactor)
	chunkReplicatedIndex:=0
	for _, chunk := range chunks {
		for j := 0; j < replicationFactor; j++ {
			chunkIDS[chunkReplicatedIndex] = chunkReplicatedIndex
			ChunksStorageMock[chunkReplicatedIndex] = chunk
			chunkReplicatedIndex++
		}
	}
	return chunkIDS

}
