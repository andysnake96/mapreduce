package main

import (
	"../aws_SDK_wrap"
	"../core"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
func MapPhaseRecovery(masterControl *core.MASTER_CONTROL, failedJobs map[int][]int, uploader *aws_SDK_wrap.UPLOADER) bool {
	println("MAP RECOVERY")
	//// filter failed workers in map results
	workerMapJobsToReassign := make(map[int][]int) //workerId--> map job to redo (chunkID previusly assigned)
	filteredMapResults := make([]core.MapWorkerArgsWrap, 0, len((*masterControl).MasterData.MapResults))
	moreFails := core.PingProbeAlivenessFilter(masterControl, false) //filter in place failed workers ( other eventually failed among calls
	///// evalutate to terminate
	if len(masterControl.WorkersAll) < core.Config.MIN_WORKERS_NUM {
		_, _ = fmt.Fprint(os.Stderr, "TOO MUCH WORKER FAILS... ABORTING COMPUTATION..")
		killAll(&masterControl.Workers)
		os.Exit(96)
	}
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
	//// checkpoint master state updating  chunk fair share assignments
	if core.Config.BACKUP_MASTER {
		backUpMasterState(masterControl, uploader)
	}
	/// retry map
	mapResultsNew, err := assignMapJobsWithRedundancy(&masterControl.MasterData, &masterControl.Workers, newChunks, masterControl.MasterData.AssignedChunkWorkersFairShare) //RPC 2,3 IN SEQ DIAGRAM
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

func MasterReplicaStart(myAddr string) {
	//master replica logic, ping probe real master, on fault read from stable storage old master state and recovery from there...
	masterDead := false
	downloader, uploader := aws_SDK_wrap.InitS3Links(core.Config.S3_REGION)

	masterAddr, err := core.GetMasterAddr(downloader, core.Config.S3_BUCKET, core.MASTER_ADDRESS_PUBLISH_S3_KEY)
	masterAddrIP := (strings.Split(masterAddr, ":"))[0]
	masterAddr = masterAddrIP + ":" + strconv.Itoa(core.Config.PING_SERVICE_BASE_PORT)
	var masterState uint32
	pollingPing := 2 * time.Millisecond * (time.Duration(core.Config.PING_TIMEOUT_MILLISECONDS))
	for {
		err, masterState = core.PingHeartBitSnd(masterAddr)
		if err != nil {
			masterDead = true
			break
		}
		if masterState == core.ENDED {
			break
		}
		time.Sleep(pollingPing)
	}
	if !masterDead {
		os.Exit(0)
	}
	masterControl, err := RecoveryFailedMasterState(downloader)
	core.CheckErr(err, true, "FAILED MASTER RECOVERY STATE")
	masterControl.MasterAddress = myAddr
	//masterControl.State = masterState
	chunks := core.InitChunks(core.FILENAMES_LOCL) //chunkize filenames
	masterControl.StateChan = make(chan uint32, 5)
	masterControl.MasterData.Chunks = chunks //save in memory loaded chunks -> they will not be backup in master checkpointing
	masterControl.MasterRpc = masterRpcInit()
	refreshConnections(&masterControl)

	//// restart master logic from loaded old master state
	masterLogic(masterControl.State, &masterControl, true, uploader)
}

const MASTER_STATE_S3_KEY = "MASTER_STATE_DUMP"
const MASTER_STATE_SIZE_EXCESS = 50000000

func backUpMasterState(control *core.MASTER_CONTROL, uploader *aws_SDK_wrap.UPLOADER) {
	//backup master state to stable storage

	var err error = nil
	base64OfState := core.SerializeMasterStateBase64(*control)
	err = aws_SDK_wrap.UploadDATA(uploader, base64OfState, MASTER_STATE_S3_KEY, core.Config.S3_BUCKET)
	println("serialized master state in stable, fault tollerant  storage")

	if core.CheckErr(err, true, "MASTER STATE BACKUP FAILED, ABORTING") {
		killAll(&control.Workers)
		os.Exit(96)
	}
}
func RecoveryFailedMasterState(downloader *aws_SDK_wrap.DOWNLOADER) (core.MASTER_CONTROL, error) {
	//recovery master state previusly uploaded to stable storage (S3)
	buf := make([]byte, MASTER_STATE_SIZE_EXCESS)
	err := aws_SDK_wrap.DownloadDATA(downloader, core.Config.S3_BUCKET, MASTER_STATE_S3_KEY, &buf, false)
	if core.CheckErr(err, false, "master old state recovery err") {
		return core.MASTER_CONTROL{}, err
	}
	out := core.DeSerializeMasterStateBase64(string(buf))
	return out, nil
}

func refreshConnections(masterControl *core.MASTER_CONTROL) {
	//// estamblish new connection to workers recovered evaluating exiting on too much fails
	workersKindsNums := map[string]int{
		core.WORKERS_MAP_REDUCE: len(masterControl.Workers.WorkersMapReduce), core.WORKERS_ONLY_REDUCE: len(masterControl.Workers.WorkersOnlyReduce),
		core.WORKERS_BACKUP_W: len(masterControl.Workers.WorkersBackup),
	}
	var sourceWorkersContainer *[]core.Worker //dest variable for workers to init
	failedWorkers := make(map[int]bool)
	for workerKind, numToREInit := range workersKindsNums {
		println("initiating: ", numToREInit, "of workers kind: ", workerKind)
		//taking worker kind addresses list
		if workerKind == core.WORKERS_MAP_REDUCE {
			sourceWorkersContainer = &(masterControl.Workers.WorkersMapReduce)
		} else if workerKind == core.WORKERS_ONLY_REDUCE {
			sourceWorkersContainer = &(masterControl.Workers.WorkersOnlyReduce)
		} else if workerKind == core.WORKERS_BACKUP_W {
			sourceWorkersContainer = &(masterControl.Workers.WorkersBackup)
		}
		for i := 0; i < numToREInit; i++ {
			// inplace restore worker rpc client
			worker := &((*sourceWorkersContainer)[i])
			workerRpcAddr := worker.Address + ":" + strconv.Itoa(worker.State.ControlRPCInstance.Port)
			client, err := rpc.Dial(core.Config.RPC_TYPE, workerRpcAddr)
			if core.CheckErr(err, false, "worker conn refresh failed") {
				worker.State.Failed = true
				failedWorkers[worker.Id] = true
				continue
			}
			worker.State.ControlRPCInstance.Client = (*core.CLIENT)(client)
		}
	}
	println("while refreshing workers connection founded failed worker: ", len(failedWorkers))
	fairShareAssignementFiltered := make(map[int][]int)
	for workerID, chunks := range masterControl.MasterData.AssignedChunkWorkersFairShare {
		_, failedWorker := failedWorkers[workerID]
		if !failedWorker {
			fairShareAssignementFiltered[workerID] = chunks // append fair share of worker only if hasn't failed during master respawn
		}
	}
	///// filter away failed workers and wait for readiness among all active workers
	core.PingProbeAlivenessFilter(masterControl, true)
	/// evaluate to exit on too much faults
	if len(masterControl.WorkersAll) < core.Config.MIN_WORKERS_NUM {
		_, _ = fmt.Fprint(os.Stderr, "TOO MUTCH FAILS...")
		killAll(&masterControl.Workers)
		os.Exit(96)
	}

}

/*

	master replica on master fault will recovery eventual lost worker result calling these 2 function
	that have the same return values of original triggers for MAP() & REDUCE() in non faulty master logic
	worker will cache answers to master for master replica spawn
*/
func RecoveryMapResults(control *core.MASTER_CONTROL) ([]core.MapWorkerArgsWrap, bool) {
	//retrieve map result from workers following fair chunk assignment and  set eventual errors
	//return retrieved result and true if something has returned fails
	mapResultReFetched := make([]core.MapWorkerArgsWrap, 0, len(control.Workers.WorkersMapReduce))
	hasFailed := false
	var err error
	var reply core.Map2ReduceRouteCost
	for workerID, chunks := range control.MasterData.AssignedChunkWorkersFairShare {
		workerPntr := core.GetWorker(workerID, &control.Workers, false)
		if workerPntr != nil {
			err = nil
			err = (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Call("CONTROL.RecoveryMapRes", chunks, &reply)
		} else {
			err = errors.New("failed worker ")
			hasFailed = true
		}
		mapResultReFetched = append(mapResultReFetched,
			core.MapWorkerArgsWrap{
				WorkerId: workerID,
				Reply:    reply,
				Err:      err,
				MapJobArgs: core.MapWorkerArgs{
					ChunkIds: chunks,
				},
			})
		if err != nil {
			hasFailed = true
		}
	}

	return mapResultReFetched, hasFailed
}

func RecoveryReduceResults(control *core.MASTER_CONTROL) bool {
	//try to recover reducers results, if some fails has happened reset them and later retry saving map computations
	fails := false
	reducersResults := make([]map[string]int, core.Config.ISTANCES_NUM_REDUCE)
	r := 0
	for redLogicID, workerID := range control.MasterData.ReducerSmartBindingsToWorkersID {
		workerPntr := core.GetWorker(workerID, &control.Workers, false)
		if workerPntr == nil {
			fails = true
			break
		}
		println("recovering final token share from reducer id :", redLogicID, "at :", workerPntr.Address)
		err := (*rpc.Client)(workerPntr.State.ControlRPCInstance.Client).Call("CONTROL.RecoveryReduceResult", redLogicID, &reducersResults[r])
		r++
		if core.CheckErr(err, false, "reduce recovery err") {
			fails = true
			break
		}
	}
	if !fails {
		//////////// aggregate if all result has been retrieved
		for _, redResult := range reducersResults {
			_ = control.MasterRpc.ReturnReduce(redResult, nil)
		}
	}
	return fails
	//TODO ALTERNATIVA COMPLICATA MA OTTIMA:
	// smazzandomi tutti i worker che chiamano reduce <-- chunksAssignmentFairShare
	// verifico se i reducer sono stati attivati, notificando il nuovo indirizzo del master replica
	// recupero risultati  generando lista di errori e eventualmente flag per REDUCE() triggerate
}
