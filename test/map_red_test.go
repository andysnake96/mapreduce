package test
//
//import (
//	"bufio"
//	"fmt"
//	"io/ioutil"
//	"os"
//	"strings"
//	"testing"
//	"../core"
//)
//
//func readFilenames(filenames []string) (string, error) {
//	//read all filenames
//	// return a concatenation of files data n and return eventual err
//	var f *os.File
//	var err error
//	fileStrings := make([]string, len(filenames))
//	for x := 0; x < len(filenames); x++ {
//		f, err = os.Open(filenames[x])
//		if err != nil {
//			return "file err",  err //propagate file not founded
//		}
//		_fileStr, err := ioutil.ReadAll(bufio.NewReader(f))
//		fileStrings[x] = string(_fileStr)
//		if err != nil {
//			return "read err",  err //propagate file not founded
//		}
//	}
//	outString := strings.Join(fileStrings, "")
//	return outString,   nil
//}
//func mapRedSingleThreadWrap()(map[string]int,error){
//	var filenames []string = core.FILENAMES_LOCL
//	////		single thread  version
//	println("SINGLE THREAD EXECUTING.....")
//	outSingleThread := make(map[string]int)
//	textRaw, err := readFilenames(filenames)
//	if err != nil {
//		return nil,err
//	}
//	// parse & count words in text by same map reduce Map function
//	mp := new(core.MapperIstanceStateInternal)
//	_ = mp.Map_parse_builtin(string(textRaw), &outSingleThread)
//	return mp.IntermediateTokens,nil
//}
//func TestMapRed(t *testing.T) {
//		//TODO DOC REWRITE BECAUSE NEW CHUNKIZATION
//		//test map readuce works by effectuing same operation by a single thread
//		//witch will read all file text (from a big UNCHUNKIZED STRING),parse this string in words and count words occurences...
//		//this result will be matched with map reduce final tokens ...
//		//!because of CHUNKIZATION with N splits in source text may cause (worst cases):
//		//	->>born of 2N splitted word (chunkization may cut a word)
//		//		->>N +2N different words values ( born word may affect other values)
//		//because of that there's an ammount of tollerate mismatches in final matching
//		//
//	//tollerableMismatches := 3 * splitAmmount
//	//tollerableNotFounded := 2 * splitAmmount
//	singleThreadOutTokens,err:=mapRedSingleThreadWrap()
//	if core.CheckErr(err,false,"") {
//		t.Fatal("FAIL ON SINGLE THREAD VERSION ")
//	}
//
//	fmt.Println("RPC MAP REDUCE MULTI core.Worker EXECUTION")
//	////	exec multithread RPC version with map reduce architecture
//	//mapReduceFinalTokens := _main(filenames)
//	fmt.Printf("MATCHING RESULTS")
//	//check if all tokens obtained by parsing are inside map created by a single parsing all file UNCHUNKIZED
//	for _, tk := range mapReduceFinalTokens {
//		singleThreadValue, PresentWord := outSingleThread[tk.K]
//		if singleThreadValue != tk.V {
//			t.Log("  MISMATCH AT KEY", tk.K, " mismatch ", tk.V, " != ", singleThreadValue)
//			tollerableMismatches--
//		}
//		if !PresentWord {
//			t.Log("  NOTPRESENT :(", tk.K)
//			tollerableNotFounded--
//		}
//	}
//	if tollerableMismatches < 0 || tollerableNotFounded < 0 {
//		t.Fatal("MISMATCHES ARE MORE THEN CHUNKIZATION EFFECT", splitAmmount)
//	}
//	fmt.Println("TEST OK!")
//}
//
//*/