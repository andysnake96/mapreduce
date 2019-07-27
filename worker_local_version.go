package main

import (
	"net/rpc"
	"strconv"
)

func initWorkers_localMock_dinamic() WorkersKinds {
	/*
		init all kind of workers with addresses and base config
		dinamically from generated addresses file at init phase
	*/
	var worker Worker
	workersOut	:=*new(WorkersKinds)
	idWorker:=0
	workersOut.workersMapReduce=make([]Worker,len(Addresses.workersMapReduce))
	for i,address:=range Addresses.workersMapReduce{
		worker=Worker{
			address: address,
			id:      idWorker,
			state: WorkerStateMasterControl{}}
		initWorkerIstances(&worker,MAP)
		workersOut.workersMapReduce[i]=worker
		idWorker++
	}
	workersOut.workersOnlyReduce=make([]Worker,len(Addresses.workersOnlyReduce))
	for i,address:=range Addresses.workersMapReduce{
		worker=Worker{
			address: address,
			id:      idWorker,
			state: WorkerStateMasterControl{}}
		initWorkerIstances(&worker,REDUCE)
		workersOut.workersOnlyReduce[i]=worker
		idWorker++
	}
	workersOut.workersBackup=make([]Worker,len(Addresses.workersBackup))
	for i,address:=range Addresses.workersBackup{
		worker=Worker{
			address: address,
			id:      idWorker,
			state: WorkerStateMasterControl{}}
		initWorkerIstances(&worker,BACKUP)
		workersOut.workersMapReduce[i]=worker
		idWorker++
	}
	return workersOut
}

func initWorkerIstances(worker *Worker,istanceType int) {
	//init instances for worker depending to the worker type

	//init chunk service istance for every worker
	id:=0			//id of istances unique only inside a worker node
	client,err:=rpc.Dial(Config.RPC_TYPE,worker.address+strconv.Itoa(Config.CHUNK_SERVICE_BASE_PORT))
	checkErr(err,true,"init worker")
	worker.state.workerIstances[id]=_workerIstanceControl{
		port:     CHUNK_INIT_BASE_PORT,
		kind:     CHUNK_ID_INIT,
		intState: IDLE,
		client:   client,
	}
	worker.state.workerNodeLinks[id]=worker			//quick backward link
	id++
	if istanceType==MAP{
		client,err:=rpc.Dial(Config.RPC_TYPE,worker.address+strconv.Itoa(Config.MAP_SERVICE_BASE_PORT))
		checkErr(err,true,"init worker")
		worker.state.workerIstances[id]=_workerIstanceControl{
			port:     Config.MAP_SERVICE_BASE_PORT,
			kind:     MAP,
			intState: IDLE,
			client:   client,
		}
		worker.state.workerNodeLinks[id]=worker			//quick backward link
		id++
	} else if istanceType==REDUCE{
		client,err:=rpc.Dial(Config.RPC_TYPE,worker.address+strconv.Itoa(Config.REDUCE_SERVICE_BASE_PORT))
		checkErr(err,true,"init worker")
		worker.state.workerIstances[id]=_workerIstanceControl{
			port:     Config.REDUCE_SERVICE_BASE_PORT,
			kind:     REDUCE,
			intState: IDLE,
			client:   client,
		}
		worker.state.workerNodeLinks[id]=worker			//quick backward link
		id++
	} else if istanceType==BACKUP{						//BACKUP=>PRE MAP ISTANCE
		client,err:=rpc.Dial(Config.RPC_TYPE,worker.address+strconv.Itoa(Config.MAP_SERVICE_BASE_PORT))
		checkErr(err,true,"init worker")
		worker.state.workerIstances[id]=_workerIstanceControl{
			port:     Config.MAP_SERVICE_BASE_PORT,
			kind:     MAP,
			intState: IDLE,
			client:   client,
		}
		worker.state.workerNodeLinks[id]=worker			//quick backward link
		id++
	}else {
		println("UNRECONIZED ISTANCE TYPE",istanceType)
	}
}

/*todo deprecated */
func initWorkers_localMock_static() WorkersKinds {
	/*
	init all kind of workers empty in local
	statically
	 */
	workersOut	:=*new(WorkersKinds)

	idBase:=0
	//init MAPPER mutable to REDUCERS
	numWorkersToInit:=Config.WORKER_NUM_MAP
	workersOut.workersMapReduce=make([]Worker,numWorkersToInit)
	insertNewWorkers(&workersOut.workersMapReduce,numWorkersToInit,idBase)
	idBase+=numWorkersToInit
	//init SEPARATED REDUCERS
	numWorkersToInit=SEPARATED_REDUCERS_NUM
	workersOut.workersOnlyReduce=make([]Worker,numWorkersToInit)
	insertNewWorkers(&workersOut.workersMapReduce,numWorkersToInit,idBase)
	idBase+=numWorkersToInit
	//init BACKUP WORKERS
	numWorkersToInit=WORKER_REPLICA_BACKUP_NUM
	workersOut.workersBackup=make([]Worker,numWorkersToInit)
	idBase+=numWorkersToInit
	insertNewWorkers(&workersOut.workersMapReduce,numWorkersToInit,idBase)

	return workersOut

}
func insertNewWorkers(workers *[]Worker, numWorkersToInit int, startID int) {
	for i:=0;i<numWorkersToInit;i++{
		address := "localhost"
		state:=new(WorkerStateMasterControl)							//TODO CHECK GO CALLOC INITIALIZATION BRAKPOINT
		*workers=append(*workers,Worker{address,i})
	}
}



func getChunksNotAlreadyAssignedRR(chunksIds *[]int, numChunkToFind int, chunksIdsAssignedAlready  []int) ([]int,error) {
	/*
	find first numChunkToFind not already present in list chunkAssignedAlready (not already assigned to worker
	 */
	chunksToAssign:=make([]int,numChunkToFind)
	for i:=0;i<len(*chunksIds)&&len(chunksToAssign)<numChunkToFind;i++{
		chunkId:=(*chunksIds)[i]
		alreadyAssignedChunk :=false
		for _,chunkIdsAssignedAlready :=range chunksIdsAssignedAlready{
			if chunkId==chunkIdsAssignedAlready {
				alreadyAssignedChunk =true
			}
		}
		if !alreadyAssignedChunk {
			chunksToAssign=append(chunksToAssign,chunkId)
		}
	}
	if len(chunksToAssign)<numChunkToFind {
		return chunksToAssign,error("not finded all replication chunk to assign")

	}
}


///// STORAGE SERVICE MOCK
var ChunksStorageMock map[int]CHUNK
func loadChunksStorageService_localMock(filenames []string) []int {
	/*
	simulate chunk distribuited storage service in a local
	chunks will be generated and a map of chunkID->chunk_data will be created
	 */
	chunks :=_init_chunks(filenames)
	chunkIDS:=make([]int,len(ChunksStorageMock))
	ChunksStorageMock=make(map[int]CHUNK)
	for i,chunk :=range chunks{
		ChunksStorageMock[i]=chunk
		chunkIDS=append(chunkIDS,i)
	}
	return chunkIDS

}
