package core

import (
	"errors"
	"net/rpc"
	"strconv"
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
