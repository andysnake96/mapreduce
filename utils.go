package mapReduce

import (
	"bufio"
	"io/ioutil"
	"os"
	"strings"
)

func check(e error) {
	//check error in one line ...TODO THRHOW EXECEPTION &?
	if e != nil {
		panic(e)
	}
}

type TEXT_FILE struct {
	filename string
	filesize int64
	///TODO BETTER ?
	numblock int
	blocks   *[]string
}

func readFile(filename string) TEXT_FILE {
	f, err := os.Open("/tmp/dat")
	check(err)
	defer f.Close()
	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()
	out := TEXT_FILE{filename: filename, filesize: fileSize} //set up return struct
	reader := bufio.NewReader(f)
	fileData, err := ioutil.ReadAll(reader) //TODO MORE EFFICIENT READ LOOP !!!!

	//// BLOCKIZE READED DATA
	div := int(fileSize / blockSize)
	rem := fileSize % blockSize
	if rem > 0 {
		div++
	}
	out.numblock = div
	for x := 0; x < div; x++ {
		lowIndx := x * blockSize
		highIndx := (x + 1) * blockSize
		block := string(fileData[lowIndx:highIndx])
		(*out.blocks)[x] = block //assign block to out var
	}
	return out
}

type token struct {
	//rappresent token middle value out from map phase
	key   string
	value int //key occurence on prj 1
}

//// HASHING KEY FUNCs
func hashKeyReducerSum(key string, maxIDOut int) int {
	//simply hash string to int in [0,maxIDOut) by sum chars and %
	sum := 0
	for c := range key {
		sum += c
	}
	return sum % maxIDOut
}

//// SORT SUPPORT FUNCTION FOR TOKEN LIST
type tokenSorter struct {
	//rappresent token middle value out from map phase
	tokens []token
	//by func(tks1,tks2 token)	//sorting function ... default by key builtin sort func
}

func (t tokenSorter) Len() int {
	return len(t.tokens)
}
func (t tokenSorter) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}
func (t tokenSorter) Less(i, j int) bool {
	return strings.Compare(t.tokens[i].key, t.tokens[j].key) == -1
}
