package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
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
		check(e)
	}
}

func _init_chunks(filenames []string) []CHUNK {
	/*
		initialize chunk structure ready to be assigned to map workers
		files will be readed in multiple threads and totalsize will be divided in fair chunks sizes
		eventually the size of the reminder of division for assignment will be assigned to last chunk
	*/
	fmt.Println("---INIT PHASE---")
	filesChunkized := make([]CHUNK, Configuration.WORKERNUMMAP) //chunkset for assignement
	filesData := make([]string, len(filenames))
	barrierRead := new(sync.WaitGroup)
	barrierRead.Add(len(filenames))
	var totalWorkSize int64 = 0
	//////	chunkize files
	for i, filename := range filenames { //evaluting total work size for fair assignement
		f, err := os.Open(filename)
		check(err)
		OpenedFiles[i] = f
		go func(barrierRead **sync.WaitGroup, destData *string) { //read all files in separated threads
			allbytes, err := ioutil.ReadAll(bufio.NewReader(f))
			check(err)
			*destData = string(allbytes)
			(*barrierRead).Done()
			runtime.Goexit()
		}(&barrierRead, &filesData[i])
		fstat, err := f.Stat()
		check(err)
		totalWorkSize += fstat.Size()
	}
	chunkSize := int64(totalWorkSize / int64(Configuration.WORKERNUMMAP)) //avg like chunk size
	reminder := int64(totalWorkSize % int64(Configuration.WORKERNUMMAP))  //assigned to first worker
	barrierRead.Wait()                                                    //wait read data end in all threads
	allStr := strings.Join(filesData, "")

	var low, high int64
	for x := 0; x < len(filesChunkized); x++ {
		low = chunkSize * int64(x)
		high = chunkSize * int64(x+1)
		filesChunkized[x] = CHUNK(allStr[low:high])
	}
	if reminder > 0 {
		filesChunkized[len(filesChunkized)-1] = CHUNK(allStr[low : high+reminder]) //last worker get bigger chunk
	}
	return filesChunkized
}

//// HASHING KEY FUNCs
func hashKeyReducerSum(key string, maxIDOut int) int {
	//simply hash string to int in [0,maxIDOut) by sum chars and %
	const EXTRASHUFFLE = 96 //extra shuffle in hash func
	sum := 0
	for c := range key {
		sum += c
	}
	sum += EXTRASHUFFLE
	return sum % maxIDOut
}

//// SORT SUPPORT FUNCTION FOR TOKEN LIST
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

/// OTHER
func check(e error) {
	//check error in one line ...TODO THRHOW EXECEPTION &?
	if e != nil {
		log.Fatal(e)
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
	check(err)
	_, err = encodeFile.Seek(0, 0)
	check(err)
	defer encodeFile.Close()
	for _, tk := range defTokens {
		line := fmt.Sprint(tk.K, "->", tk.V, "\r\n")
	write:
		n, err = encodeFile.WriteString(line[lw:])
		check(err)
		if n < len(line) {
			lw += n
			fmt.Println("write short...")
			goto write
		}
		lw = 0
	}
}

func ReadConfigFile() {
	f, err := os.Open(CONFIGFILENAME)
	//f, err := os.Open("/home/andysnake/Scrivania/uni/magistrale/DS/src/mapReduce/config.json")
	check(err)
	defer f.Close()
	//configRawStr,err:=ioutil.ReadAll(bufio.NewReader(f))
	//check(err)
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&Configuration)
	check(err)
	//assign global var for readed configuration

}
