package main

import (
	"../aws_SDK_wrap"
	"../core"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func ReducersReplacementRecovery(failedWorkersReducers map[int][]int, oldReducersBindings map[int]int, workersKinds *core.WorkersKinds) map[int]int {
	//re decide reduce placement on workers selecting a random healty worker
	//old bindings modified in place
	//returned new bindings and updated in place old bindings
	failedReducersIDs := make([]int, 0, len(failedWorkersReducers))
	newReducersPlacements := make(map[int]int) //new placement for failed reducers
	// setup workers in preferential order of reducer reschedule
	workers := append(workersKinds.WorkersOnlyReduce, workersKinds.WorkersBackup...)
	workers = append(workers, workersKinds.WorkersMapReduce...)
	//filter active workers and bindings
	for _, worker := range workers {
		reducersIDs, failedWorker := failedWorkersReducers[worker.Id]
		failedWorker = worker.State.Failed || failedWorker
		if failedWorker {
			worker.State.Failed = true
			for i := 0; i < len(reducersIDs); i++ {
				delete((oldReducersBindings), reducersIDs[i]) //remove stale assignement
			}
		}
	}
	for _, reducerIDs := range failedWorkersReducers {
		failedReducersIDs = append(failedReducersIDs, reducerIDs...)
	}
	core.GenericPrint(failedReducersIDs, "failed reducersIDs: ")
	/// assign failed reducers to workers
	reducersPerWoker := 1
	reducersPerWokerResidue := 0
	if len(failedReducersIDs) > len(workers) {
		reducersPerWoker = len(failedReducersIDs) / len(workers)
		reducersPerWokerResidue = len(failedReducersIDs) % len(workers)
	}
	reducerToAssign := 0
	for i, worker := range workers {
		reducersForWorker := reducersPerWoker
		if i == len(workers)-1 {
			reducersForWorker += reducersPerWokerResidue
		}
		for i := 0; i < reducersForWorker; i++ {
			failedReducerID := failedReducersIDs[reducerToAssign+i]
			///FINALLY RE PLACE FAILED REDUCER
			newReducersPlacements[failedReducerID] = worker.Id //newly replaced reducer
			(oldReducersBindings)[failedReducerID] = worker.Id
		}
		reducerToAssign += reducersPerWoker
		if reducerToAssign >= len(failedReducersIDs) {
			break
		}
	}
	return newReducersPlacements
}

/*func AssignMapWorksRecovery(failedWorkerMapJobs map[int][]int, workersKinds *core.WorkersKinds, oldChunksWorkersAssignement *map[int][]int) map[int][]int {
	//reassign map jobs exploiting previusly chunk replication, so map jobs will be instantiated on worker with necessary chunk
	chunkToReMap := make([]int, 0, len(failedWorkerMapJobs))
	newMapAssignements := make(map[int][]int) //new assigmenets only map: worker-->mapJobs to redo
	workers := append(workersKinds.WorkersBackup, workersKinds.WorkersMapReduce...)
	///filter active worker
	for indx, worker := range workers {
		//get if worker is actually failed
		_, failedWorker := failedWorkerMapJobs[worker.Id]
		failedWorker = worker.State.Failed || failedWorker
		if failedWorker {
			worker.State.Failed = true
			workers = append((workers)[:indx], (workers)[indx+1:]...) //remove failed worker
			delete((*oldChunksWorkersAssignement), worker.Id)         //remove stale assignement
		}
	}
	//get list of map jobs to reschedule
	for _, chunks := range failedWorkerMapJobs {
		chunkToReMap = append(chunkToReMap, chunks...)
	}

	//// assign map jobs exploiting chunks replication on workers
	tmpAssignement := []int{-1, -1} //temporary assignement workerID-->#assignements
	for _, chunkId := range chunkToReMap {
		for _, worker := range workers {
			for _, chunkIdAssigned := range worker.State.ChunksIDs { //search match of prev. assigned chunk<->chunk to reMAP
				if chunkIdAssigned == chunkId {
					///evalutate best assignement among all possibilities
					alreadyAssignedToWorker := len(newMapAssignements[worker.Id])
					if tmpAssignement[0] == -1 || alreadyAssignedToWorker > tmpAssignement[1] { //first assignement or better assignement value
						tmpAssignement[0] = worker.Id
						tmpAssignement[1] = alreadyAssignedToWorker
					} //else prev assignement evaluated better then this possibility
				}
			}
		}
		//do the best assignement selected or random if not exist a replication chunk among active workers
		foundedWorkerWithChunk := tmpAssignement[0] != -1
		if foundedWorkerWithChunk {
			newMapAssignements[tmpAssignement[0]] = append(newMapAssignements[tmpAssignement[0]], chunkId)
		} else {
			workerIndx := rand.Intn(len(workers)) //TODO AVOID REPLICATION
			workerRnd := (workers)[workerIndx]
			newMapAssignements[workerRnd.Id] = append(newMapAssignements[workerRnd.Id], chunkId)
			//because of new chunk donwload on randomly selected worker-> update chunk assignement
			(*oldChunksWorkersAssignement)[workerRnd.Id] = append((*oldChunksWorkersAssignement)[workerRnd.Id], chunkId)
			workerRnd.State.ChunksIDs = append(workerRnd.State.ChunksIDs, chunkId)
		}
	}

	return newMapAssignements

}
*/
func MapPhaseRecovery(masterControl *core.MASTER_CONTROL, failedJobs map[int][]int) bool {
	//// filter failed workers in map results
	workerMapJobsToReassign := make(map[int][]int) //workerId--> map job to redo (chunkID previusly assigned)
	filteredMapResults := make([]core.MapWorkerArgsWrap, 0, len((*masterControl).MasterData.MapResults))
	moreFails := core.PingProbeAlivenessFilter(masterControl) //filter in place failed workers ( other eventually failed among calls
	for _, mapResult := range (*masterControl).MasterData.MapResults {
		errdMap := core.CheckErr(mapResult.Err, false, "WORKER id:"+strconv.Itoa(mapResult.WorkerId)+" ON MAPS JOB ASSIGN")
		failedWorker := errdMap || moreFails[mapResult.WorkerId]
		if failedWorker {
			workerMapJobsToReassign[mapResult.WorkerId] = mapResult.MapJobArgs.ChunkIds //schedule to reassign only not redundant share of chunks on worker
		} else {

			filteredMapResults = append(filteredMapResults, mapResult)
		}
	}
	if failedJobs != nil { //eventually insert passed failed jobs
		for workerID, jobs := range failedJobs {
			workerMapJobsToReassign[workerID] = jobs
		}
	}
	((*masterControl).MasterData.MapResults) = filteredMapResults
	checkMapRes(masterControl)
	//re assign failed map job exploiting chunk replication among workers
	newChunks := AssignChunksIDsRecovery(&masterControl.Workers, workerMapJobsToReassign, (masterControl.MasterData.AssignedChunkWorkers), (masterControl.MasterData.AssignedChunkWorkersFairShare))
	checkMapRes(masterControl)
	/// retry map
	mapResultsNew, err := assignMapJobsWithRedundancy(&masterControl.MasterData, &masterControl.Workers, newChunks) //RPC 2,3 IN SEQ DIAGRAM
	masterControl.MasterData.MapResults = append(masterControl.MasterData.MapResults, mapResultsNew...)
	if err {
		_, _ = fmt.Fprintf(os.Stderr, "error on map RE assign\n aborting all")
		print(&mapResultsNew)
		return false
	}
	return true
}
func AssignChunksIDsRecovery(workerKinds *core.WorkersKinds, workerChunksFailed map[int][]int,
	oldAssignementsGlbl, oldAssignementsFairShare map[int][]int) map[int][]int {
	checkMapRes(&MasterControl)
	//reassign chunks evaluating existent replication old assignmenets and reducing assignement only at fundamental chunk to reassign
	//old assignements map will be modified in place

	failedChunk := make(map[int]bool, len(oldAssignementsGlbl))
	workers := append(workerKinds.WorkersBackup, workerKinds.WorkersMapReduce...) //preferential order for chunk re assignement
	workers = append(workers, workerKinds.WorkersOnlyReduce...)                   //preferential order for chunk re assignement
	//extract chunks to reassign filtering duplications introduced for fault tollerant (less reassignment here)
	for workerId, _ := range workerChunksFailed {
		delete(oldAssignementsGlbl, workerId)      //DELETE FAILED WORKER KEY FROM ASSIGNEMENT DICT
		delete(oldAssignementsFairShare, workerId) //DELETE FAILED WORKER KEY FROM ASSIGNEMENT DICT
	}

	reverseGlblChunkMap := make(map[int]int)
	for workerID, chunks := range oldAssignementsGlbl {
		for _, chunkID := range chunks {
			reverseGlblChunkMap[chunkID] = workerID
		}
	}
	reverseChunkMapFairShare := make(map[int]int)
	backup := make(map[int][]int)
	for workerID, chunks := range oldAssignementsFairShare {
		backup[workerID] = chunks
		core.GenericPrint(chunks, "worker:"+strconv.Itoa(workerID))
		for _, chunkID := range chunks {
			reverseChunkMapFairShare[chunkID] = workerID
		}
	}
	///// select foundamental chunk to reassign in accord with prev replication, eventually append to fair share missing chunk
	for _, chunks := range workerChunksFailed {
		for _, chunkId := range chunks {
			workerInPossess, isAlreadyAssigned := reverseGlblChunkMap[chunkId]
			_, presentInFairShare := reverseChunkMapFairShare[chunkId]
			if !isAlreadyAssigned && !presentInFairShare {
				failedChunk[chunkId] = true
			} else if !presentInFairShare { //update fair share with prev. redundant chunk now useful
				oldAssignementsFairShare[workerInPossess] = append(oldAssignementsFairShare[workerInPossess], chunkId)

				reverseChunkMapFairShare[chunkId] = workerInPossess
				println("exploited data locality of chunk: ", chunkId, "on worker ", workerInPossess)
			}
		}
	}
	//// only not already assigned chunk will be scheduled for re download on other active workers
	unrecoverableChunks := make([]int, 0, len(workerChunksFailed))
	for chunkID, _ := range failedChunk {
		unrecoverableChunks = append(unrecoverableChunks, chunkID)
	}
	core.GenericPrint(unrecoverableChunks, "CHUNKS TO REASSIGN")

	//select avaible worker not failed and set new assignement until all foundamental chunk are assigned or not enought workers
	chunksToReassignPerWorker := 1
	chunksToReassignPerWorkerResidiue := 0
	if len(unrecoverableChunks) > len(workers) {
		chunksToReassignPerWorker = int(len(unrecoverableChunks) / len(workers))
		chunksToReassignPerWorkerResidiue = len(unrecoverableChunks) % len(workers)
	}
	chunkToAssignIndexEnd := 0
	assignedChunks := 0
	newAssignements := make(map[int][]int)
	checkMapRes(&MasterControl)
	for i := 0; i < len(workers) && assignedChunks < len(unrecoverableChunks); i++ {
		chunkToAssignIndexEnd = assignedChunks + chunksToReassignPerWorker
		if i == len(workers)-1 {
			chunkToAssignIndexEnd += chunksToReassignPerWorkerResidiue //for last worker eventually assign residue jobs
		}
		chunksPerWorker := unrecoverableChunks[assignedChunks:chunkToAssignIndexEnd]
		workerId := (workers)[i].Id
		//// assignement recovery
		core.GenericPrint(chunksPerWorker, "assigning to : "+strconv.Itoa(workerId))
		newAssignements[workerId] = chunksPerWorker                                                             //only new assignements map set
		(oldAssignementsGlbl)[workerId] = append((oldAssignementsGlbl)[workerId], chunksPerWorker...)           //update in place assignements
		(oldAssignementsFairShare)[workerId] = append((oldAssignementsFairShare)[workerId], chunksPerWorker...) //update in place assignements

		assignedChunks += chunksToReassignPerWorker
	} //compleated assignements
	checkMapRes(&MasterControl)
	return newAssignements
}
func mergeMapResults(mapResBase []core.MapWorkerArgsWrap, mapRes2 []core.MapWorkerArgsWrap) []core.MapWorkerArgsWrap {
	//merge a base list of map resoults filtering failed maps job and appending mapRes2 to it
	mapResultsMerged := make([]core.MapWorkerArgsWrap, 0, len(mapResBase))
	for _, mapperRes := range mapResBase {
		if mapperRes.Err == nil { //good result from map output
			mapResultsMerged = append(mapResultsMerged, mapperRes)
		}
	}
	return append(mapResultsMerged, mapRes2...)
}

var MASTER_BACKUP_PING_POLLING time.Duration = time.Millisecond

func MasterReplicaStart(masterAddr string) {
	//master replica logic, ping probe real master, on fault read from stable storage old master state and recovery from there...
	masterDead := false
	downloader, uploader := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)
	masterAddr, err := core.GetMasterAddr(downloader, core.Config.S3_BUCKET, core.MASTER_ADDRESS_PUBLISH_S3_KEY)
	var masterState uint32
	for {
		err, masterState = core.PingHeartBitSnd(masterAddr)
		if err != nil {
			masterDead = true
			break
		}
		if masterState == core.ENDED {
			break
		}
		time.Sleep(MASTER_BACKUP_PING_POLLING)
	}
	if !masterDead {
		os.Exit(0)
	}

	masterControl, err := RecoveryFailedMasterState(downloader)
	core.CheckErr(err, true, "FAILED MASTER RECOVERY STATE")
	masterControl.MasterAddress = masterAddr
	masterControl.State = masterState
	refreshConnections(&masterControl)
	//// restart master logic from loaded master state downloaded
	masterLogic(masterState, &masterControl, uploader)
}

const MASTER_STATE_S3_KEY = "MASTER_STATE_DUMP"
const MASTER_STATE_SIZE_EXCESS = 20000

func backUpMasterState(control *core.MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) error {
	//backup master state to stable storage
	var err error = nil
	//base64OfState:=core.SerializeMasterStateBase64(*control)
	//err=aws_SDK_wrap.UploadDATA(uploader,base64OfState,MASTER_STATE_S3_KEY,core.Config.S3_BUCKET)
	println("serialized master state in stable, fault tollerant  storage")
	return err
}
func RecoveryFailedMasterState(downloader *aws_SDK_wrap.DOWNLOADER) (core.MASTER_CONTROL, error) {
	//recovery master state previusly uploaded to stable storage (S3)
	buf := make([]byte, MASTER_STATE_SIZE_EXCESS)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, MASTER_STATE_S3_KEY, buf, false)
	if core.CheckErr(err, false, "master old state recovery err") {
		return core.MASTER_CONTROL{}, err
	}
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] != 0 {
			buf = buf[:i+1]
		}
	}
	out := core.DeSerializeMasterStateBase64(string(buf))
	return out, nil
}

func refreshConnections(masterControl *core.MASTER_CONTROL) {
	//// aggreagate worker registration with 2! for
	workersKindsNums := map[string]int{
		core.WORKERS_MAP_REDUCE: len(masterControl.Workers.WorkersMapReduce), core.WORKERS_ONLY_REDUCE: len(masterControl.Workers.WorkersOnlyReduce),
		core.WORKERS_BACKUP_W: len(masterControl.Workers.WorkersBackup),
	}
	var destWorkersContainer *[]core.Worker //dest variable for workers to init
	failedWorkers := 0
	for workerKind, numToREInit := range workersKindsNums {
		println("initiating: ", numToREInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == core.WORKERS_MAP_REDUCE {
			destWorkersContainer = &(masterControl.Workers.WorkersMapReduce)
		} else if workerKind == core.WORKERS_ONLY_REDUCE {
			destWorkersContainer = &(masterControl.Workers.WorkersOnlyReduce)
		} else if workerKind == core.WORKERS_BACKUP_W {
			destWorkersContainer = &(masterControl.Workers.WorkersBackup)
		}
		for i := 0; i < numToREInit; i++ {
			// inplace restore worker rpc client
			worker := &((*destWorkersContainer)[i])
			workerRpcAddr := worker.Address + ":" + strconv.Itoa(worker.State.ControlRPCInstance.Port)
			client, err := rpc.Dial(core.Config.RPC_TYPE, workerRpcAddr)
			if core.CheckErr(err, false, "worker conn refresh failed") {
				worker.State.Failed = true
				failedWorkers++
				continue
			}
			worker.State.ControlRPCInstance.Client = (*core.CLIENT)(client)
			//client.Go("CONTROL.NewMaster",masterControl.MasterAddress,nil,nil) //TODO IDEMPOTENT WORKER
			//todo other?
		}
	}
	if failedWorkers > 0 && masterControl.State == core.CHUNK_ASSIGN { //quick filter failed worker for re assignement without old computed data
		core.PingProbeAlivenessFilter(masterControl)
	}
	println("while refreshing workers connection founded failed worker: ", failedWorkers)
}
