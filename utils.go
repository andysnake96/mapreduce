package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
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

func ReadConfigFile() {
	f, err := os.Open(CONFIGFILENAME)
	check(err)
	defer f.Close()
	//configRawStr,err:=ioutil.ReadAll(bufio.NewReader(f))
	//check(err)
	decoder := json.NewDecoder(f)
	configuration := Configuration
	err = decoder.Decode(&configuration)
	check(err)
	//assign global var for readed configuration

}
