package mapReduce

import (
	"fmt"
	"strings"
)

/*
core functions for map-reduce works
map has been implemented in differente way adding method to a _map structure
 */
type _map struct {
	rawChunck string
	output_hshmp map[string]int
}
func (m _map) map_string_builtin{
//map operation for a worker, from assigned chunk string produce tokens key value
//used string builtin split ... something like 2 chunk all chars read
	m.output_hshmp = make(map[string]int)
	//producing token splitting rawChunck by \n and \b
	lines := strings.Split(m.rawChunck,"\n")
	const _WRD4LINE = 12
	words := make([]string,len(lines)*_WRD4LINE)	//preallocate
	for _,l := range lines {
		words = append(words,strings.Split(l," ") ...) //TODO SLICES CONCATENATION GO PERFORMACE?
	}
	for _,w := range words {
		m.output_hshmp[w]++							//set token in a dict to simply handle key repetitions
	}
	//TODO OUTPUT IN output dict field of _map
}
func (m _map) map_raw_parse{
	//map op for a worker parsing raw chunk in words
	m.output_hshmp = make(map[string]int)
	//parser states
	const STATE_WORD = 0
	const STATE_NOTWORD = 1
	state := STATE_WORD
	var char byte
	//words low delimiter index in chunk
	wordDelimLow :=0
	//set initial state
	char = m.rawChunck[0]			//get first char
	if char == '\n' || char== ' ' {
		state=STATE_NOTWORD
	}
	//PRODUCING OUTPUT TOKEN HASHMAP IN ONLY 1! READ OF chunk chars...
	for i:=0;i<len(m.rawChunck);i++ { //iterate among chunk chars
		char= m.rawChunck[i]
		isWordChr := char!= ' ' && char != '\n' //char is part of a word
		if state==STATE_WORD && !isWordChr { //split condition
			word:=m.rawChunck[wordDelimLow:i+1]
			m.output_hshmp[word]++			 //set token key in out hashmap
		}
		//TODO ELIF LIKE... ALREADY EXCLUSIVE CONDITIONS
		if state==STATE_NOTWORD && isWordChr {
			wordDelimLow=i 					//set new word low index delimiter
		}
	}
	fmt.Println("DEBUG DICTIONARY OF TOKENS-->",m.output_hshmp)
	//TODO OUTPUT INSIDE STRUCTURE
}

func _reduce(middleTokens []token) map[string]int64 {
//reduce operation for a worker,
// reduce a token list to a single list of tokens without key repetitions
	outTokens := make(map[string]int64)
	for _,tk:=range middleTokens {
		outTokens[tk.key]++
	}
	return outTokens //TODO RETURN TOKENS DIRECTLY as a list of tuple... ?
}