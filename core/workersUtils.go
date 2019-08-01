package core

import (
	"errors"
	"strconv"
)

////WORKERS UTIL FUNCTIONS
func WorkerNodeWithMapper(mapperID int, workers *WorkersKinds) (*Worker, error) { //MASTER SIDE
	//return worker istance ref containing Mapper with mapperID among master ref to worker istances
	var workerNodeWithMapperIstance *Worker
	var present bool
	for _, worker := range workers.WorkersMapReduce { //find worker node
		_, present = worker.State.WorkerIstances[mapperID]
	}
	for _, worker := range workers.WorkersBackup { //find worker node
		_, present = worker.State.WorkerIstances[mapperID]
	}

	if !present {
		return nil, errors.New("NOT FOUNDED WORKER NODE WITH MAP ISTANCE WITH ID " + strconv.Itoa(mapperID))
	}
	return workerNodeWithMapperIstance, nil
}
func NumHealthyReducerOnWorker(workerNode *Worker) int {
	//return number of Reducer istances healthy  on workerNOde
	numHealthyReducers := 0
	for _, istanceState := range workerNode.State.WorkerIstances {
		if istanceState.IntState != FAILED {
			numHealthyReducers++
		}
	}
	return numHealthyReducers
}
