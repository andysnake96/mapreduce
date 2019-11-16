package main

//generate final tokens on same input of distribuited version for matching output results
import (
	"../core"
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TEST_TOKENS_OUTPUT_FILE = "TEST_TOKENS_OUTPUT_FILE"
	SINGLE_READ_VERSION     = iota
	MULTY_ROUTINE_CHUNKS_VERSION
)

/*type MASTER_CONTROLA struct {
	MasterRpc     *core.MasterRpc
	MasterAddress string
	Workers       core.WorkersKinds //connected workers
	WorkersAll    []core.Worker     //list of all workers ref.
	//list of all avaibles  chunks
	MasterData core.MASTER_STATE_DATA
	////////// master fault tollerant
	StateChan     chan uint32
	State         uint32
	pingConn      net.Conn
	UploaderState *aws_SDK_wrap.UPLOADER
}
func testEncode2() {
	m := core.MASTER_CONTROL{
		MasterRpc: &core.MasterRpc{
			FinalTokens:     []core.Token{core.Token{"a", 1}},
			ReturnedReducer: nil,
		},
		MasterAddress: "192.168.1.96",
		Workers: core.WorkersKinds{
			WorkersMapReduce: []core.Worker{{
				Address:         "1.1.1.1.1",
				PingServicePort: 0,
				Id:              0,
				State: core.WorkerStateMasterControl{
					ChunksIDs: []int{1, 2, 3},
					ControlRPCInstance: core.WorkerIstanceControl{
						Id:       1,
						Port:     2,
						Kind:     0,
						IntState: 0,
						Client:   nil,
					},
					Failed: false,
				},
			}},
			WorkersOnlyReduce: nil,
			WorkersBackup:     nil,
		},
		WorkersAll: nil,
		StateChan:  nil,
		State:      0,
		pingConn:   nil,
	}
	o := &core.MASTER_CONTROL{}
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(m)
	core.CheckErr(err, true, "encode")
	str := base64.StdEncoding.EncodeToString(b.Bytes())

	by, err := base64.StdEncoding.DecodeString(str)
	core.CheckErr(err, true, "decode base 64")
	bb := bytes.Buffer{}
	bb.Write(by)
	d := gob.NewDecoder(&bb)
	err = d.Decode(&o)
	core.CheckErr(err, true, "decode base 64")

	//core.GenericPrint(deserialized)
}
*/
func main() {
	core.Config = new(core.Configuration)
	core.ReadConfigFile(core.CONFIGFILEPATH, core.Config)

	//testEncode2()
	//os.Exit(0)
	startTime := time.Now()

	finalTokens := concurrentMap()
	//finalTokens := singleBlockMap()

	endTime := time.Now()
	tk := core.TokenSorter{finalTokens}
	sort.Sort(sort.Reverse(tk))
	core.SerializeToFile(finalTokens, TEST_TOKENS_OUTPUT_FILE)
	println("elapsed: ", endTime.Sub(startTime).String())
	errs := checkDifferenceInFinaleTokens(core.FINAL_TOKEN_FILENAME, TEST_TOKENS_OUTPUT_FILE)
	println("End Check")
	if len(errs) > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}
func checkDifferenceInFinaleTokens(tokensFile1, tokensFile2 string) []error {
	eventualError := make([]error, 0)
	/////// getting raw data from out tokens files
	file1, err1 := os.Open(tokensFile1)
	file2, err2 := os.Open(tokensFile2)
	core.CheckErrs([]error{err1, err2}, true, "open errors")
	data1, err1 := ioutil.ReadAll(bufio.NewReader(file1))
	data2, err2 := ioutil.ReadAll(bufio.NewReader(file2))
	core.CheckErrs([]error{err1, err2}, true, "readErr")

	////// parsing data from files
	var tokens1, tokens2 map[string]int
	barrier := new(sync.WaitGroup)
	barrier.Add(2)

	go func(waitG **sync.WaitGroup) {
		tokens1 = parseTokenFileData(string(data1))
		barrier.Done()
	}(&barrier)
	go func(waitG **sync.WaitGroup) {
		tokens2 = parseTokenFileData(string(data2))
		barrier.Done()
	}(&barrier)

	barrier.Wait()
	if len(tokens1) != len(tokens2) {
		err := errors.New("different len in dicts")
		_, _ = fmt.Fprint(os.Stderr, err.Error(), " \n")
		eventualError = append(eventualError, err)
	}
	for key, value := range tokens1 {
		_, presentInOtherDict := tokens2[key]
		if !presentInOtherDict {
			err := errors.New("absent key:" + key + "in  OutTokens2")
			_, _ = fmt.Fprint(os.Stderr, err.Error(), "\n")
			eventualError = append(eventualError, err)
			continue
		}
		if value != tokens2[key] {
			err := errors.New("different values in dicts for key " + key)
			_, _ = fmt.Fprint(os.Stderr, key, "different values in dicts 1<->2", math.Abs(float64(value-tokens2[key])), "\n")
			eventualError = append(eventualError, err)
		}
		//println(key,value,tokens2[key])
	}
	return eventualError
}
func parseTokenFileData(data string) map[string]int {
	tks := strings.Split(data, "\r\n")
	outTokenMap := make(map[string]int)
	for _, t := range tks {
		if t == "" {
			continue //avoid last blankline
		}
		tk := strings.Split(t, "->")
		key := tk[0]
		val, _ := strconv.Atoi(tk[1])
		outTokenMap[key] = val
	}
	return outTokenMap
}
func singleBlockMap() []core.Token {

	filenames := core.FILENAMES_LOCL
	onlyChunk := ""
	for x := 0; x < len(filenames); x++ {
		f, err := os.Open(filenames[x])
		if err != nil {
			panic("open err")
		}
		_fileStr, err := ioutil.ReadAll(bufio.NewReader(f))
		if err != nil {
			panic("read err")
		}
		onlyChunk += string(_fileStr)
	}
	mapOut := make(map[string]int)
	mapper := new(core.MapperIstanceStateInternal)
	_ = mapper.Map_parse_builtin(onlyChunk, &mapOut)
	finalTokens := make([]core.Token, len(mapOut))
	i := 0
	for key, value := range mapOut {
		finalTokens[i] = core.Token{
			K: key,
			V: value,
		}
		i++
	}
	return finalTokens
}
func concurrentMap() []core.Token {
	//multi routine map over chunk
	chunks := core.InitChunks(core.FILENAMES_LOCL)
	mappersOutput := make([]map[string]int, len(chunks))

	barrier := new(sync.WaitGroup)
	barrier.Add(len(chunks))
	for i, _ := range chunks {
		mapper := new(core.MapperIstanceStateInternal)
		go func(waitG **sync.WaitGroup, indx int) {
			_ = mapper.Map_parse_builtin(string(chunks[indx]), &(mappersOutput[indx]))
			(*waitG).Done()
		}(&barrier, i)
	}
	barrier.Wait()
	finalTokensMap := make(map[string]int)
	for _, partialTokens := range mappersOutput {
		for key, value := range partialTokens {
			finalTokensMap[key] += value
		}
	}

	finalTokens := make([]core.Token, len(finalTokensMap))
	i := 0
	for key, value := range finalTokensMap {
		finalTokens[i] = core.Token{
			K: key,
			V: value,
		}
		i++
	}
	return finalTokens
}
