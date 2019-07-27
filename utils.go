package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

type Token struct {
	//rappresent Token middle V out from map phase
	K string
	V int //Key occurence on prj 1
}

type CHUNK string

func cleanUpFiles(files []*os.File) {
	for _, f := range files {
		e := f.Close()
		checkErr(e, true, "")
	}
}

func _init_chunks(filenames []string) []CHUNK {	//fast concurrent file read for chunk generation
	/*
		initialize chunk structure ready to be assigned to map workers
		files will be readed in multiple threads and totalsize will be divided in fair chunks sizes
		eventually the size of the reminder of division for assignment will be assigned to last chunk
	*/
	fmt.Println("---start chunkization---")
	filesChunkized := make([]CHUNK, Config.WORKER_NUM_MAP) //chunkset for assignement
	filesData := make([]string, len(filenames))
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	var totalWorkSize int64 = 0
	//////	chunkize files
	for i, filename := range filenames { //evaluting total work size for fair assignement
		f, err := os.Open(filename)
		checkErr(err, true, "")
		OpenedFiles[i] = f
		go func(barrierRead **sync.WaitGroup, destData *string) { //read all files in separated threads
			allbytes, err := ioutil.ReadAll(bufio.NewReader(f))
			checkErr(err, true, "")
			*destData = string(allbytes)
			(*barrierRead).Done()
			runtime.Goexit()
		}(&barrierRead, &filesData[i])
		fstat, err := f.Stat()
		checkErr(err, true, "")
		totalWorkSize += fstat.Size()
	}
	chunkSize := int64(totalWorkSize / int64(Config.WORKER_NUM_MAP)) //avg like chunk size
	reminder := int64(totalWorkSize % int64(Config.WORKER_NUM_MAP))  //assigned to first Worker
	barrierRead.Wait()                                               //wait read data end in all threads
	allStr := strings.Join(filesData, "")

	var low, high int64
	for x := 0; x < len(filesChunkized); x++ {
		low = chunkSize * int64(x)
		high = chunkSize * int64(x+1)
		filesChunkized[x] = CHUNK(allStr[low:high])
	}
	if reminder > 0 {
		filesChunkized[len(filesChunkized)-1] = CHUNK(allStr[low : high+reminder]) //last Worker get bigger chunk
	}
	return filesChunkized
}

//// HASHING KEY FUNCs
func hashKeyReducerSum(key string, maxIDOut int) int {
	//simply hash string to int in [0,maxIDOut) by sum chars and %
	//for the given key string will return the ID of dest reducer
	const EXTRASHUFFLE = 96 //extra shuffle in hash func
	sum := 0
	for c := range key {
		sum += c
	}
	sum += EXTRASHUFFLE
	return sum % maxIDOut
}

//// SORT_FINAL SUPPORT FUNCTION
// FOR TOKEN LIST
type tokenSorter struct {
	//rappresent Token middle V out from map phase
	tokens []Token
	//by func(tks1,tks2 Token)	//sorting function ... default by Key builtin sort func
}

func (t tokenSorter) Len() int {
	return len(t.tokens)
}
func (t tokenSorter) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}
func (t tokenSorter) Less(i, j int) bool {
	return t.tokens[i].V < t.tokens[j].V
}

////  FOR ROUTING COSTS
type RoutingCostsSorter struct {
	routingCosts []TrafficCostRecord
}

func (r RoutingCostsSorter) Len() int {
	return len(r.routingCosts)
}
func (r RoutingCostsSorter) Swap(i, j int) {
	r.routingCosts[i],r.routingCosts[j] = r.routingCosts[j], r.routingCosts[i]
}
func (r RoutingCostsSorter) Less(i, j int) bool {
	return r.routingCosts[i].routingCost < r.routingCosts[j].routingCost
}

/////	HEARTBIT FUNCS		/////
func pingHeartBitRcv(){
	//TODO PING receve
}
func pingHeartBitSnd(){
	//TODO PING send
	//TODO CHEAP TRIGGER ON NO ANSW
}

/// OTHER
func checkErrs(errs []error, fatal bool, supplementMsg string) {
	for _,e:=range errs{
		checkErr(e,fatal,supplementMsg)
	}
}
func checkErr(e error, fatal bool, supplementMsg string) {
	//check error, exit if fatal is true
	baseMsg:=e.Error()
	if e != nil {
		if(fatal==true){
			log.Fatal(baseMsg+supplementMsg,e)
		} else {
			log.Println(baseMsg+supplementMsg,e)
		}
	}
}
func max(a int, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

func serializeToFile(defTokens []Token, filename string) {
	/////	SERIALIZE RESULT TO FILE
	n := 0
	lw := 0
	encodeFile, err := os.Create(filename)
	checkErr(err, true, "")
	_, err = encodeFile.Seek(0, 0)
	checkErr(err, true, "")
	defer encodeFile.Close()
	for _, tk := range defTokens {
		line := fmt.Sprint(tk.K, "->", tk.V, "\r\n")
	write:
		n, err = encodeFile.WriteString(line[lw:])
		checkErr(err, true, "")
		if n < len(line) {
			lw += n
			fmt.Println("write short...")
			goto write
		}
		lw = 0
	}
}
func listOfDictCumulativeSize(dictList []map[int]int) int {
	cumulativeSum:=0
	for _,dict :=range dictList{
		cumulativeSum+=len(dict)
	}
	return cumulativeSum
}



func ReadConfigFile(configFileName string,destVar ConfigInterface)  {
	f, err := os.Open(configFileName)
	checkErr(err, true, "config file open")
	defer f.Close()
	//configRawStr,err:=ioutil.ReadAll(bufio.NewReader(f))
	decoder := json.NewDecoder(f)
	err = decoder.Decode(destVar)
	checkErr(err, true, "")

}

func reflectionFieldsGet(strct interface{})  {
	val := reflect.ValueOf(strct)
	values := make(map[string]interface{}, val.NumField())
	metaTypes := val.Type()
	for i := 0; i < val.NumField(); i++ {
		values[metaTypes.Field(i).Name] = val.Field(i).Interface()
	}

	fmt.Println(values)

}
