package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"sync"
)

type Token struct {
	//rappresent Token middle V out from map phase
	K string
	V int //Key occurence on prj 1
}

type TEXT_FILE struct {
	//filename string
	filesize int64
	numblock int
	blocks   []string
}

func chunksAmmount(f *os.File) (int, int64) {
	//return ammount of blocks needed for file  f
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	div := int(fileSize / blockSize)
	rem := fileSize % blockSize
	if rem > 0 {
		div++
	}
	return div, fileSize
}

func readFile(f *os.File) TEXT_FILE {

	//INIT TEXT STRUNCT FIELDS
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	numChunk, _ := chunksAmmount(f)
	out := TEXT_FILE{filesize: fileSize, numblock: numChunk} //set up return struct
	out.blocks = make([]string, out.numblock)                //allocate space for blocks
	//READ DATA
	reader := bufio.NewReader(f)
	fileData, err := ioutil.ReadAll(reader) //TODO confirm readall wrap a good read loop
	check(err)
	//CHUNKIZE
	for x := 0; x < out.numblock; x++ {
		lowIndx := x * blockSize
		highIndx := (x + 1) * blockSize
		block := string(fileData[lowIndx:highIndx])
		out.blocks[x] = block //assign block to out var
	}
	return out
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
	return strings.Compare(t.tokens[i].K, t.tokens[j].K) == -1
}

/// OTHER
func check(e error) {
	//check error in one line ...TODO THRHOW EXECEPTION &?
	if e != nil {
		panic(e)
	}
}
func max(a int, b int) int {
	return int(math.Max(float64(a), float64(b)))
}

func _init_file_structs(file *os.File, chunksDest *TEXT_FILE, barrier *sync.WaitGroup) {
	//read filename in a struct TEXT_FILE separating text in chunks by pointer chunksDest
	*chunksDest = readFile(file)
	barrier.Done()
}
func serializeToFile(defTokens []Token, filename string) {
	/////	SERIALIZE RESULT TO FILE
	n := 0
	lw := 0
	encodeFile, err := os.Create(filename)
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
	//if err != nil {
	//	panic(err)
	//}
	//e := gob.NewEncoder(encodeFile)
	//
	//// Encoding the map
	//er := e.Encode(defTokens)
	//check(er)
	//encodeFile.Close()
}
