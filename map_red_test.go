package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func _readFilenames(filenames []string) (string, int, error) {
	//read all filenames
	// return a concatenation of files data, ammount of split effectuated in chunkization and return eventual err
	var numSeparations int = 0
	var f *os.File
	var err error
	fileStrings := make([]string, len(filenames))
	for x := 0; x < len(filenames); x++ {
		f, err = os.Open(filenames[x])
		if err != nil {
			return "file err", -1, err //propagate file not founded
		}
		_fileStr, err := ioutil.ReadAll(bufio.NewReader(f))
		fileStrings[x] = string(_fileStr)
		if err != nil {
			return "read err", -1, err //propagate file not founded
		}
		numSeparations = Configuration.WORKERNUMMAP - 1 //ammount of split effectuated during chunkizing in map readuce
	}
	outString := strings.Join(fileStrings, "")
	return outString, numSeparations, nil
}

func TestMapRed(t *testing.T) {
	/*
		test map readuce works by effectuing same operation by a single thread
		witch will read all file text (from a big UNCHUNKIZED STRING),parse this string in words and count words occurences...
		this result will be matched with map reduce final tokens ...
		!because of CHUNKIZATION with N splits in source text may cause (worst cases):
			->>born of 2N splitted word (chunkization may cut a word)
				->>N +2N different words values ( born word may affect other values)
		because of that there's an ammount of tollerate mismatches in final matching
	*/
	//TODO CHECK ENV PATH FROM TEST EXEC
	var filenames []string = []string{"txtSrc/1012-0.txt", "txtSrc/1017-0.txt", "txtSrc/2834-0.txt", "txtSrc/pg17405.txt", "txtSrc/pg174.txt"}
	ReadConfigFile()
	////		single thread  version
	fmt.Println("SINGLE THREAD EXECUTING.....")
	outSingleThread := make(map[string]int)
	textRaw, splitAmmount, err := _readFilenames(filenames)
	if err != nil {
		t.Fatal("ERROR OCCURRED DURING FILE READS", err)
	}
	// parse & count words in text by same map reduce Map function
	mp := new(_map)
	_ = mp.Map_parse_builtin(string(textRaw), &outSingleThread)
	fmt.Println("RPC MAP REDUCE MULTI WORKER EXECUTION")
	////	exec multithread RPC version with map reduce architecture
	mapReduceFinalTokens := _main(filenames)
	tollerableMismatches := 3 * splitAmmount
	tollerableNotFounded := 2 * splitAmmount
	fmt.Printf("MATCHING RESULTS")
	//check if all tokens obtained by parsing are inside map created by a single parsing all file UNCHUNKIZED
	for _, tk := range mapReduceFinalTokens {
		singleThreadValue, PresentWord := outSingleThread[tk.K]
		if singleThreadValue != tk.V {
			t.Log("  MISMATCH AT KEY", tk.K, " mismatch ", tk.V, " != ", singleThreadValue)
			tollerableMismatches--
		}
		if !PresentWord {
			t.Log("  NOTPRESENT :(", tk.K)
			tollerableNotFounded--
		}
	}
	if tollerableMismatches < 0 || tollerableNotFounded < 0 {
		t.Fatal("MISMATCHES ARE MORE THEN CHUNKIZATION EFFECT", splitAmmount)
	}
	fmt.Println("TEST OK!")
}
