package core

import (
	"errors"
	"net/rpc"
	"strconv"
	"sync"
)

////WORKERS UTIL FUNCTIONS



func GetWorker(id int, workers *WorkersKinds) (Worker, error) {
	//return worker with id, nil if not found
	for _, worker := range (*workers).WorkersMapReduce {
		if worker.Id == id {
			return worker, nil
		}
	}
	for _, worker := range (*workers).WorkersOnlyReduce {
		if worker.Id == id {
			return worker, nil
		}
	}
	for _, worker := range (*workers).WorkersBackup {
		if worker.Id == id {
			return worker, nil
		}
	}
	return Worker{}, errors.New("NOT FOUNDED WORKER :" + strconv.Itoa(id))
}
func downloadChunk(chunkId int, waitGroup **sync.WaitGroup, chunkLocation *CHUNK) {
	/*
		download chunk from data store, allowing concurrent download with waitgroup to notify downloads progress
		chunk will be written in given location, thread safe if chunkLocation is isolated and readed only after waitgroup has compleated
	*/
	if Config.LOCAL_VERSION {
		chunk, present := ChunksStorageMock[chunkId]
		if !present {
			panic("NOT PRESENT CHUNK IN MOCK\nidchunk: " + strconv.Itoa(chunkId)) //TODO ROBUSTENESS PRE EBUG
		}
		*chunkLocation = chunk //write chunk to his isolated position
	} //else //TODO DOWNLOAD FROM S3, CONFIG FILE AND S3 SDK.... only mem--> S3 rest
	(*waitGroup).Done() //notify other chunk download compleated
}

func routeInfosCombiner(mappersRouteCosts Map2ReduceRouteCost, workerAggregateRouteCosts *Map2ReduceRouteCost) {
	for reducerId, routeCost := range mappersRouteCosts.RouteCosts {
		workerAggregateRouteCosts.RouteCosts[reducerId] += routeCost
	}
	for reducerId, _ := range mappersRouteCosts.RouteNum {
		workerAggregateRouteCosts.RouteNum[reducerId]++
	}

}
func InitRpcClients(addresses map[int]string) (map[int]*rpc.Client, error) {
	clients := make(map[int]*rpc.Client, len(addresses))
	var err error
	for k, v := range addresses {
		clients[k], err = rpc.Dial(Config.RPC_TYPE, v)
		if CheckErr(err, false, "dialing") {
			return nil, err
		}
	}
	return clients, nil
}
func estimateTokenSize(token Token) int {
	//return unsafe.Sizeof(token.V)+unsafe.Sizeof(token.K[0])*len(token.K)	//TODO CAST ERR
	return len(token.K) + 4
}
func getChunk(chunkID int, workerChunksStore *WorkerChunks) CHUNK {
	//TODO GET CHUNK FROM CHUNKS DOWNLOADED void chunk if not present in chunks store
	//because of go map concurrent access allowing has variated among different version a mutex will be taken for the access
	workerChunksStore.Mutex.Lock()
	chunk, present := workerChunksStore.Chunks[chunkID]
	workerChunksStore.Mutex.Unlock()
	if !present {
		return CHUNK("")
	}
	return chunk
}
