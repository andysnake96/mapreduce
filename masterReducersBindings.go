package main

import (
	"sort"
)

/*
module of functions supporting Reducers Workers Istances placement into Workers network
with the target of exploiting data locality to minimize traffic cost of routing intermediate token (MAP() out) to reducers
Costraint of LoadBalacing and Fault Tollerant will be considered during functions
*/
type ReducersTrafficCosts struct {
	//list of mapper traffic costs to reducersID of intermediate data
	//for each dest reducerID(K) the cost(V) is the cumulation of the whole expected traffic to itself
	trafficCostsReducersDict []map[int]int
	//reflect the distribuition of intermediate MAP data to route to reducers among mappers
}
type TrafficCostRecord struct{
	//traffic cost of data routing from a mapper to a reducer
	//reflect an edge in a bipartite graph (G) weighted on edges reflecting data routing cost between mappers->reducers
	routingCost int
	mapperID int
	reducerID int
}

func reducersBindingsLocallityAwareEuristic(reducersIdsTrafficIN ReducersTrafficCosts) (map[int]string){
	/*
	Quick Euristic to select placement of Reducers Workers Istances exploiting intermediate tokens data locality
	minimizing traffic cost of routing data  to reducers will be produced the bindings of reducersID->address (worker node placement)
	Basic Algo: extract a list of costs for each hash binding to a reducerID
				sort decreasingly by costs
				*select first ISTANCES_NUM_REDUCE records to avoid in REDUCE route phase
	(Graph theory simmetry: assuming a bipartite graph (G) weighted on edges reflecting data routing cost between mappers->reducers
		will be "contracted" first ISTANCES_NUM_REDUCE edges more expensive finding a partition of G )
	 */
	reducersTrafficsCostListSorted := extractCostsListSorted(reducersIdsTrafficIN)
	reducersBindings:=make(map[int]string)	//final binding of reducerID -> actual worker address placement

	const MAX_REDUCERS_PER_WORKER = 2                                     //Max contraction per worker possible
	MaxContractions := Config.ISTANCES_NUM_REDUCE -SEPARATED_REDUCERS_NUM //max Num of reducers contractions
	contractedR := 0
	for i := 0; i<len(reducersTrafficsCostListSorted)||contractedR<MaxContractions;i++  {
		record :=reducersTrafficsCostListSorted[i]
		if reducersBindings[record.reducerID]==""{				  //NOT ALREADY CONTRACTED THE REDUCER
			workerNode,err:=workerNodeWithMapper(record.mapperID)
			checkErr(err,true,"")
			if numHealthyReducerOnWorker(workerNode)<=MAX_REDUCERS_PER_WORKER{	//NOT TOO MUCH CONTRACTION ON SAME WORKER
				reducersBindings[record.reducerID]=workerNode.address		//CONTRACT IF COSTRAINT OKK
				contractedR++
			}
		}
	}
	return reducersBindings
}

func extractCostsListSorted(trafficCosts ReducersTrafficCosts) []TrafficCostRecord {
	//build list of traffics costs
	trafficCostsRecords:=make([]TrafficCostRecord,listOfDictCumulativeSize(trafficCosts.trafficCostsReducersDict))
	for mapperID,reducerRoutCosts:=range trafficCosts.trafficCostsReducersDict{
		for reducerID,routingCost :=range reducerRoutCosts{
			trafficCostsRecords=append(trafficCostsRecords,TrafficCostRecord{routingCost,mapperID,reducerID})
		}
	}
	//sort list by cost
	routingCostsSorter:=RoutingCostsSorter{trafficCostsRecords}
	sort.Sort(sort.Reverse(routingCostsSorter))

	return trafficCostsRecords

}

//TODO PLI script wrap di formulazione su appunti :==))))
