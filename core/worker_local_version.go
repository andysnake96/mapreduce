package core

import (
	"errors"
	"sort"
)

var AssignedPortsAll []int //list of assigned ports (globally) for local version

func NextUnassignedPort(basePort int, assignedPorts *[]int, assignNewPort bool, checkExternalPortBindings bool) int {
	//find next avaible port among the assignedPorts starting search from basePort,
	// will be returned the closest next assignable port
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
	println("founded avaible port ", port)
	if assignNewPort { //evalue to append port to appended port list
		*assignedPorts = append(*assignedPorts, port)
	}
	return port
}

func InitWorkers_localMock() WorkersKinds {
	/*
		init all kind of workers on localhost under a progressive assigned ports using MaxPortAssigned var
		explicit assignement of istances for workers by their kind for future change on deflt assignements
	*/
	var worker Worker
	var port int
	workersOut := *new(WorkersKinds)
	idWorker := 0
	workersOut.WorkersMapReduce = make([]Worker, len(Addresses.WorkersMapReduce))
	for i, address := range Addresses.WorkersMapReduce {
		idInstance := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}

		port = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
		initWorkerInstancesRef(&worker, port, idInstance, CHUNK_ID_INIT)
		idInstance++
		port = NextUnassignedPort(Config.MAP_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
		initWorkerInstancesRef(&worker, port, idInstance, MAP)
		idInstance++
		workersOut.WorkersMapReduce[i] = worker
		idWorker++
	}
	workersOut.WorkersOnlyReduce = make([]Worker, len(Addresses.WorkersOnlyReduce))
	for i, address := range Addresses.WorkersOnlyReduce {
		idInstance := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}
		port = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
		initWorkerInstancesRef(&worker, port, idInstance, CHUNK_ID_INIT)
		idInstance++
		port = NextUnassignedPort(Config.REDUCE_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
		initWorkerInstancesRef(&worker, port, idInstance, REDUCE)
		idInstance++
		workersOut.WorkersOnlyReduce[i] = worker
		idWorker++

	}
	workersOut.WorkersBackup = make([]Worker, len(Addresses.WorkersBackup))
	for i, address := range Addresses.WorkersBackup {
		idInstance := 0
		worker = Worker{
			Address: address,
			Id:      idWorker,
			State:   WorkerStateMasterControl{}}
		port = NextUnassignedPort(Config.CHUNK_SERVICE_BASE_PORT, &AssignedPortsAll, true, false)
		initWorkerInstancesRef(&worker, port, idInstance, CHUNK_ID_INIT)
		idInstance++
		workersOut.WorkersBackup[i] = worker
		idWorker++
	}
	return workersOut
}

func GetChunksNotAlreadyAssignedRR(chunksIds *[]int, numChunkToFind int, chunksIdsAssignedAlready []int) ([]int, error) {
	/*
		find first numChunkToFind not already present in list chunkAssignedAlready (not already assigned to worker
	*/
	chunksToAssign := make([]int, numChunkToFind)
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
		chunks will be generated and a map of chunkID->chunk_data will be created
	*/
	chunks := Init_chunks(filenames)
	chunkIDS := make([]int, len(ChunksStorageMock))
	ChunksStorageMock = make(map[int]CHUNK)
	for i, chunk := range chunks {
		ChunksStorageMock[i] = chunk
		chunkIDS = append(chunkIDS, i)
	}
	return chunkIDS

}
