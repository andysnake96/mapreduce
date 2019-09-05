package main

import (
	"../core"
	"math/rand"
)

func ReducersReplacementRecovery(failedWorkersReducers map[int][]int, newMapBindings map[int][]int, oldReducersBindings *map[int]int,workersKinds *core.WorkersKinds) map[int]int {
	//re decide reduce placement on workers selecting a random healty worker
	failedReducersIDs:=make([]int,0,len(failedWorkersReducers))
	newReducersPlacements :=make(map[int]int) //new placement for failed reducers
	// setup workers in preferential order of reducer reschedule
	workers:= append(workersKinds.WorkersBackup, workersKinds.WorkersOnlyReduce...)
	workers= append(workers, workersKinds.WorkersMapReduce...)

	//filter active workers and bindings
	for indx, worker := range workers {
		reducersIDs,failedWorker:=failedWorkersReducers[worker.Id]
		failedWorker=worker.State.Failed || failedWorker
		if failedWorker {
			//TODO PING PROBE
			worker.State.Failed=true
			workers=append((workers)[:indx],(workers)[indx+1:]...)	//remove failed worker
			for i := 0; i < len(reducersIDs); i++ {
				delete((*oldReducersBindings),reducersIDs[i])		//remove stale assignement
			}
		}
	}
	for _, reducerIDs := range failedWorkersReducers {
		failedReducersIDs = append(failedReducersIDs, reducerIDs...)
	}
	/// assign failed reducers to workers starting from BACKUP
	reducersPerWoker:=1
	reducersPerWokerResidue:=0
	if len(failedReducersIDs) > len(workers) {
		reducersPerWoker=len(failedReducersIDs)/len(workers)
		reducersPerWokerResidue=len(failedReducersIDs)%len(workers)
	}
	reducerToAssign :=0
	for i, worker := range workers {
		reducersForWorker:=reducersPerWoker
		if i==len(workers)-1 {
			reducersPerWoker+=reducersPerWokerResidue
		}
		for i := 0; i < reducersForWorker; i++ {
			failedReducerID:=failedReducersIDs[reducerToAssign+i]
			///FINALLY RE PLACE FAILED REDUCER
			newReducersPlacements[failedReducerID]=worker.Id //newly replaced reducer
			(*oldReducersBindings)[failedReducerID]=worker.Id
		}
		reducerToAssign +=reducersPerWoker
	}
	return newReducersPlacements
}

func AssignMapWorksRecovery(failedWorkerMapJobs map[int][]int, workersKinds *core.WorkersKinds, oldChunksWorkersAssignement *map[int][]int) map[int][]int {
	//reassign map jobs exploiting previusly chunk replication, so map jobs will be instantiated on worker with necessary chunk
	chunkToReMap:=make([]int,0,len(failedWorkerMapJobs))
	newMapAssignements:=make(map[int][]int)							//new assigmenets only map: worker-->mapJobs to redo
	workers:= append(workersKinds.WorkersBackup, workersKinds.WorkersMapReduce...)
	///filter active worker
	for indx, worker := range workers {
		//get if worker is actually failed
		_,failedWorker:=failedWorkerMapJobs[worker.Id]
		failedWorker=worker.State.Failed || failedWorker
		if failedWorker {
			worker.State.Failed=true
			workers=append((workers)[:indx],(workers)[indx+1:]...)	//remove failed worker
			delete((*oldChunksWorkersAssignement),worker.Id)		//remove stale assignement
		}
	}
	//get list of map jobs to reschedule
	for _, chunks := range failedWorkerMapJobs {
		chunkToReMap = append(chunkToReMap, chunks...)
	}

	//// assign map jobs exploiting chunks replication on workers
	tmpAssignement:=[]int{-1,-1}											//temporary assignement workerID-->#assignements
	for _, chunkId := range chunkToReMap {
		for _, worker := range workers {
			for _, chunkIdAssigned := range worker.State.ChunksIDs {		//search match of prev. assigned chunk<->chunk to reMAP
				if chunkIdAssigned==chunkId {
					///evalutate best assignement among all possibilities
					alreadyAssignedToWorker:=len(newMapAssignements[worker.Id])
					if tmpAssignement[0]==-1 || alreadyAssignedToWorker>tmpAssignement[1] { //first assignement or better assignement value
						tmpAssignement[0]=worker.Id
						tmpAssignement[1]=alreadyAssignedToWorker
					}	//else prev assignement evaluated better then this possibility
				}
			}
		}
		//do the best assignement selected or random if not exist a replication chunk among active workers
		foundedWorkerWithChunk:=tmpAssignement[0]!=-1
		if(foundedWorkerWithChunk){
			newMapAssignements[tmpAssignement[0]] = append(newMapAssignements[tmpAssignement[0]], chunkId)
		} else {
			workerIndx:=rand.Intn(len(workers))		//TODO AVOID REPLICATION
			workerRnd:=(workers)[workerIndx]
			newMapAssignements[workerRnd.Id] = append(newMapAssignements[workerRnd.Id], chunkId)
			//because of new chunk donwload on randomly selected worker-> update chunk assignement
			(*oldChunksWorkersAssignement)[workerRnd.Id] = append((*oldChunksWorkersAssignement)[workerRnd.Id], chunkId)
			workerRnd.State.ChunksIDs = append(workerRnd.State.ChunksIDs, chunkId)
		}
	}

	return newMapAssignements


}

func AssignChunksIDsRecovery(workerKinds *core.WorkersKinds,workerChunksToReassign map[int][]int,oldAssignements *(map[int][]int)) (map[int][]int) {
	//reassign chunks evaluating existent replication old assignmenets and reducing assignement only at fundamental chunk to reassign
	//old assignements map will be modified in place
	chunksFoundamentalToReassign:=make([]int,0,len(workerChunksToReassign));
	failedWorkersId :=make([]int,0,len(workerChunksToReassign))
	failedChunk :=make([]int,0,len(workerChunksToReassign))
	newAssignements:=make(map[int][]int)								//map to old only new reassignements to return
	workers:=append(workerKinds.WorkersBackup,workerKinds.WorkersMapReduce...)	//preferential order for chunk re assignement
	////filter failed workers from all workers list
	for indx, worker := range workers {
		_,failedWorker:=workerChunksToReassign[worker.Id]
		if failedWorker {
			worker.State.Failed=true
			workers=append((workers)[:indx],(workers)[indx+1:]...)
		}
	}
	for workerId, chunkIds := range workerChunksToReassign {	//extract chunks to reassign
		failedWorkersId = append(failedWorkersId, workerId)
		failedChunk = append(failedChunk, chunkIds...)
		delete(*oldAssignements, workerId) //DELETE FAILED WORKER KEY FROM ASSIGNEMENT DICT
	}
	alreadyAssignedChunksOtherWorkers:=make(map[int]int)	//reverse map: chunk -> assigned worker not failed
	for workerId, chunkIds := range *oldAssignements {
		_,present:=workerChunksToReassign[workerId]
		if !present {
			for _, chunk := range chunkIds {
				alreadyAssignedChunksOtherWorkers[chunk]=workerId
			}
		}
	}
	for i := 0; i < len(failedChunk)-1; i++ {
		chunkReplicated:=false
		for j := i+1; j < len(failedChunk); j++ {
			if failedChunk[i]== failedChunk[j] {
				chunkReplicated=true
				break
			}
		}
		_,alreadyAssigned:=alreadyAssignedChunksOtherWorkers[failedChunk[i]]
		if !chunkReplicated && ! alreadyAssigned {	//not reassign replicated chunk or already assigned to other worker (prev replication)
			chunksFoundamentalToReassign = append(chunksFoundamentalToReassign, failedChunk[i])
		}
	}
	_,lastAlreadyAssigned:=alreadyAssignedChunksOtherWorkers[failedChunk[len(failedChunk)-1]]
	if !lastAlreadyAssigned {
		chunksFoundamentalToReassign = append(chunksFoundamentalToReassign, failedChunk[len(failedChunk)-1]) //last always in (is the one witch always may force skip other chunks)
	}
	println("CHUNKS TO REASSIGN:")
	core.GenericPrint(chunksFoundamentalToReassign)

	//select avaible worker not failed and set new assignement until all foundamental chunk are assigned or not enought workers
	chunksToReassignPerWorker:=1
	chunksToReassignPerWorkerResidiue:=0
	if len(chunksFoundamentalToReassign)>len(workers) {
		chunksToReassignPerWorker = int(len(chunksFoundamentalToReassign) / len(workers))
		chunksToReassignPerWorkerResidiue = len(workers) % len(chunksFoundamentalToReassign)
	}
	assignedChunks:=0
	var chunkToAssignIndexEnd=0
	for i := 0; i < len(workers); i++ {
		chunkToAssignIndexEnd=assignedChunks+chunksToReassignPerWorker
		if i==len(workers)-1{
			chunkToAssignIndexEnd+=chunksToReassignPerWorkerResidiue	//for last worker eventually assign residue jobs
		}
		chunksPerWorker:=chunksFoundamentalToReassign[assignedChunks:chunkToAssignIndexEnd]
		workerId:= (workers)[i].Id
		(*oldAssignements)[workerId]=chunksPerWorker			//update in place assignements
		newAssignements[workerId]=chunksPerWorker				//only new assignements map set
		assignedChunks+=chunksToReassignPerWorker
	}	//compleated assignements
	return newAssignements
}
func mergeMapResults(mapResBase []MapWorkerArgs, mapRes2 []MapWorkerArgs) []MapWorkerArgs {
	//merge a base list of map resoults filtering failed maps job and appending mapRes2 to it
	mapResultsMerged:=make([]MapWorkerArgs,0,len(mapResBase))
	for _, mapperRes := range mapResBase {
		if mapperRes.err==nil {	//good result from map output
			mapResultsMerged = append(mapResultsMerged, mapperRes)
		}
	}
	return append(mapResultsMerged,mapRes2...)
}