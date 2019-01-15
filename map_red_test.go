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
		numSeparations = chunksAmmount(f) - 1 //ammount of split effectuated during chunkizing in map readuce
	}
	outString := strings.Join(fileStrings, "")
	return outString, numSeparations, nil
}
func TestMapRed(t *testing.T) {
	/*
		test map readuce works by effectuing same operation by a single thread
		single thread will read all file text (from a big UNCHUNKIZED STRING)
		parse this string in words and count words occurences...
		this result will be matched with map reduce final tokens ...
		because of CHUNKIZATION with N splits in source text may cause born of 2N splitted word
			(not present in source text files)
		==> so up to N words may be absent from map reduce result tokens
			and up to 2N words may be not present in real result (splitted borned words)
	*/
	var filenames []string = os.Args[1:]
	outSingleThread := make(map[string]int)
	textRaw, splitAmmount, err := _readFilenames(filenames)
	if err != nil {
		t.Fatal("ERROR OCCURRED DURING FILE READS", err)
	}
	//exec single thread version...simply parse & count words in text
	mp := new(_map)
	mp.Map_raw_parse(string(textRaw), &outSingleThread)

	//exec multithread RPC version with map reduce architecture
	mapReduceFinalTokens := _main(filenames)

	//check if all tokens obtained by parsing are inside map created by a single parsing all file UNCHUNKIZED
	for i, tk := range mapReduceFinalTokens {
		singleThreadValue, notPresentWord := outSingleThread[tk.K]
		if singleThreadValue != tk.V {
			t.Fatal("FATAL MISMATCH AT KEY", tk.K, "token Index", i, " mismatch ", tk.V, " != ", singleThreadValue)
		}
		if notPresentWord {
			splitAmmount--
		}
	}
	if splitAmmount < 0 {
		t.Fatal("MISMATCHES ARE MORE THEN CHUNKIZATION EFFECT")
	}
	fmt.Println("ALL TOKENS PRODUCED BY MAP&REDUCE ARE FOUNDED BY A SIMPLE PARSE OF DATA")
}
