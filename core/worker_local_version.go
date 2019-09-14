package core

import (
	"errors"
	"math/rand"
	"net/rpc"
	"sort"
	"strconv"
	"sync"
)

var AssignedPortsAll []int //list of assigned ports (globally) for local version
func NextUnassignedPort(basePort int, assignedPorts *[]int, assignNewPort bool, checkExternalPortBindings bool, networkToCheck string) int {
	//find next avaible port among the assignedPorts starting search from basePort,
	//will be returned the closest next assignable port
	//the new port will be assigned if true assignNewFoundedPort

	sort.Ints(*assignedPorts)
	conflict := false
	var nextPortAssigned int
	port := basePort
	portIndex := 0
	//check if base port is assigned
	for portIndex, nextPortAssigned = range *assignedPorts {
		if nextPortAssigned == port {
			port++ //first try to avoid port collision
			conflict = true
			break
		}
	}
	if checkExternalPortBindings && !CheckPortAvaibility(port, networkToCheck) {
		conflict = true
	}
	if conflict {
		if portIndex+1 < len(*assignedPorts) { //on port assignement conflict=>find next avaible port starting from that location
			//also skip port collision sub resolve if nextPortAssigned is already the last one
			portIndex++ //start search next port checking next assigned port
			for lastAssignedPort := (*assignedPorts)[len(*assignedPorts)-1]; nextPortAssigned < lastAssignedPort; portIndex++ {
				nextPortAssigned = (*assignedPorts)[portIndex]
				if nextPortAssigned != port { //find port avaibility near to the baseport
					if !checkExternalPortBindings {
						goto foundedPort
					}
					for ; port < nextPortAssigned && !CheckPortAvaibility(port, networkToCheck); port++ { //check port binding to other app
						println("already bounded port:", port)
						*assignedPorts = append(*assignedPorts, port) //append not assignable port
					}
					if CheckPortAvaibility(port, networkToCheck) {
						goto foundedPort
					} else {
						*assignedPorts = append(*assignedPorts, port) //append not assignable port
						continue                                      //exceeded port during search, skip increment because already done in check port bounded by other apps
					}
				}
				port++
			}

		} else {
			for ; checkExternalPortBindings && !CheckPortAvaibility(port, networkToCheck); port++ {
				*assignedPorts = append(*assignedPorts, port)
			}
		}
	}
foundedPort:
	if assignNewPort { //evalue to append port to appended port list
		*assignedPorts = append(*assignedPorts, port)
	}
	println("founded avaible port ", port, "\t", checkExternalPortBindings, "\n assigned ports:")
	GenericPrint(*assignedPorts, "")
	return port
}

func InitWorkers_LocalMock_WorkerSide(workers *[]Worker_node_internal, stopPingChan chan bool) {
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
				ReducersClients: make([]*rpc.Client, Config.ISTANCES_NUM_REDUCE),
				ExitChan:        make(chan bool),
				Id:              workerId,
				MasterAddr:      "localhost",
			}
			//starting worker control rpc instance
			avaiblePort = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, true, "tcp") //TODO HP AVAIBILITY FOR BASE PORT ASSIGNMENTS
			e, _ := InitRPCWorkerIstance(nil, avaiblePort, CONTROL, &workerNode)
			CheckErr(e, true, "instantiating base instanc error...")

			// evaluating special fields basing on worker type
			if workerKind == WORKERS_MAP_REDUCE {
				workerNode.IntermediateDataAggregated = AggregatedIntermediateTokens{
					ChunksSouces:                 make([]int, 0),
					PerReducerIntermediateTokens: make([]map[string]int, Config.ISTANCES_NUM_REDUCE),
				}
				workerNode.ReducersClients = make([]*rpc.Client, Config.ISTANCES_NUM_REDUCE)
			} //else if .....
			println("started worker: ", workerId)
			(*workers)[workerId] = workerNode
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

	workersKindsNums := map[string]int{
		WORKERS_MAP_REDUCE: Config.WORKER_NUM_MAP, WORKERS_ONLY_REDUCE: Config.WORKER_NUM_ONLY_REDUCE, WORKERS_BACKUP_W: Config.WORKER_NUM_BACKUP_WORKER,
	}
	//generic destination variable for init
	var destWorkersContainer *[]Worker //dest variable for workers to init
	//init out variables
	workersOut := *new(WorkersKinds) //actual workers to return
	workersOut.WorkersMapReduce = make([]Worker, Config.WORKER_NUM_MAP)
	workersOut.WorkersOnlyReduce = make([]Worker, Config.WORKER_NUM_ONLY_REDUCE)
	workersOut.WorkersBackup = make([]Worker, Config.WORKER_NUM_BACKUP_WORKER)
	idWorker := 0
	for workerKind, numToInit := range workersKindsNums {
		println("initiating: ", numToInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == WORKERS_MAP_REDUCE {
			destWorkersContainer = &workersOut.WorkersMapReduce
		} else if workerKind == WORKERS_ONLY_REDUCE {
			destWorkersContainer = &workersOut.WorkersOnlyReduce
		} else if workerKind == WORKERS_BACKUP_W {
			destWorkersContainer = &workersOut.WorkersBackup
		}
		for i := 0; i < len(*destWorkersContainer); i++ {

			port = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, false, "tcp")
			pingPort := NextUnassignedPort(Config.PING_SERVICE_BASE_PORT, &AssignedPortsAll, true, false, "tcp")
			client, err := rpc.Dial(Config.RPC_TYPE, worker.Address+":"+strconv.Itoa(port))
			CheckErr(err, true, "init worker client")
			//init control rpc instance
			worker = Worker{
				Address:         "localhost",
				PingServicePort: pingPort,
				Id:              idWorker,
				State: WorkerStateMasterControl{
					ChunksIDs: make([]int, 0),
					ControlRPCInstance: WorkerIstanceControl{
						Port:   port,
						Kind:   CONTROL,
						Client: (*CLIENT)(client),
					}}}

			//setting refs
			//noinspection ALL ... workers holders initiated before for
			(*destWorkersContainer)[i] = worker
			idWorker++
			println("initiated worker: ", worker.Id)
		}
	}
	workersAllsRef := append(workersOut.WorkersBackup, workersOut.WorkersMapReduce...)
	workersAllsRef = append(workersAllsRef, workersOut.WorkersOnlyReduce...)
	return workersOut, workersAllsRef
}

func GetChunksNotAlreadyAssignedRR(chunksIds []int, numChunkToFind int, chunksIdsAssignedAlready []int) ([]int, error) {
	/*
		find  numChunkToFind not already present in list chunkAssignedAlready (not already assigned to worker
	*/
	chunksToAssignable := make([]int, 0, len(chunksIds)-len(chunksIdsAssignedAlready))
	chunksToAssign := make([]int, numChunkToFind)
	for i := 0; i < len(chunksIds); i++ {
		chunkId := (chunksIds)[i]
		alreadyAssignedChunk := false
		for _, chunkIdsAssignedAlready := range chunksIdsAssignedAlready {
			if chunkId == chunkIdsAssignedAlready {
				alreadyAssignedChunk = true
			}
		}
		if !alreadyAssignedChunk {
			chunksToAssignable = append(chunksToAssignable, chunkId)
		}
	}
	for i := 0; i < numChunkToFind; i++ {
		redundantChunkToAssignIndx := rand.Intn(len(chunksToAssignable))
		chunksToAssign[i] = chunksToAssignable[redundantChunkToAssignIndx]
		/// remove assignable chunk from list
		if redundantChunkToAssignIndx == (len(chunksToAssignable) - 1) {
			chunksToAssignable = chunksToAssignable[:redundantChunkToAssignIndx]
		} else {
			chunksToAssignable = append(chunksToAssignable[:redundantChunkToAssignIndx], chunksToAssignable[redundantChunkToAssignIndx+1:]...)
		}
	}
	if len(chunksToAssignable) < numChunkToFind {
		return chunksToAssignable, errors.New("not finded all replication chunk to assign")

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
func LoadChunks_multipleTimes_debug(filenames []string, replicationFactor int) []int {
	/*
		simulate chunk distribuited storage service in a local
		chunks will be generated and a map of ChunkID->chunk_data will be created
		each chunk will be replicated replciaitonFactor number of times for debugging check
	*/
	chunks := InitChunks(filenames)
	chunkIDS := make([]int, len(chunks)*replicationFactor)
	ChunksStorageMock = make(map[int]CHUNK, len(chunks)*replicationFactor)
	chunkReplicatedIndex := 0
	for _, chunk := range chunks {
		for j := 0; j < replicationFactor; j++ {
			chunkIDS[chunkReplicatedIndex] = chunkReplicatedIndex
			ChunksStorageMock[chunkReplicatedIndex] = chunk
			chunkReplicatedIndex++
		}
	}
	return chunkIDS

}
