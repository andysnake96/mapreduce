package main

//core functions of map reduce,
//different map and reduce method for map and reduce types
import (
	"fmt"
	"strings"
)

// 		MAP		/////////////////////7
type _map int //type map methods interface

func (m *_map) Map_string_builtin(rawChunck string, tokens *map[string]int) error {
	//map operation for a worker, from assigned chunk string produce tokens key value
	//used string builtin split ... something like 2 chunk all chars read
	*tokens = make(map[string]int)
	//producing Token splitting rawChunck by \n and \b
	lines := strings.Split(rawChunck, "\n")
	const _WRD4LINE = 12
	words := make([]string, len(lines)*_WRD4LINE) //preallocate
	for _, l := range lines {
		words = append(words, strings.Split(l, " ")...) //TODO SLICES CONCATENATION GO PERFORMACE?
	}
	for _, w := range words {
		(*tokens)[w]++ //set Token in a dict to simply handle key repetitions
	}
	return nil
	//TODO OUTPUT IN output dict field of _map
}
func (m *_map) Map_raw_parse(rawChunck string, tokens *map[string]int) error {
	//map op for a worker parsing raw chunk in words
	*tokens = make(map[string]int)
	//parser states
	const STATE_WORD = 0
	const STATE_NOTWORD = 1
	state := STATE_WORD
	var char byte
	//words low delimiter index in chunk
	wordDelimLow := 0
	//set initial state
	char = rawChunck[0] //get first char
	if char == '\n' || char == ' ' {
		state = STATE_NOTWORD
	}
	//PRODUCING OUTPUT TOKEN HASHMAP IN ONLY 1! READ OF chunk chars...
	for i := 0; i < len(rawChunck); i++ { //iterate among chunk chars
		char = rawChunck[i]
		isWordChr := char != ' ' && char != '\n' //char is part of a word
		if state == STATE_WORD && !isWordChr {   //split condition
			word := rawChunck[wordDelimLow : i+1]
			(*tokens)[word]++ //set Token key in out hashmap
		}
		//TODO ELIF LIKE... ALREADY EXCLUSIVE CONDITIONS
		if state == STATE_NOTWORD && isWordChr {
			wordDelimLow = i //set new word low index delimiter
		}
	}
	fmt.Println("DEBUG DICTIONARY OF TOKENS-->", *tokens)
	//TODO OUTPUT INSIDE STRUCTURE
	return nil
}

//					//// REDUCE ///////////////
type _reduce int
type ReduceArg struct {
	key    string
	values []int
}

func (r *_reduce) Reduce_tokens_key(args ReduceArg, outToken *Token) error { //version indicated in paper
	//reduce op by values of a single key
	//return final Token with unique key string
	count := 0
	for x := 0; x < len(args.values); x++ {
		count += args.values[x]
	}
	*outToken = Token{args.key, count}
	return nil
}

//TODO EXTRA ... ASK BEFORE IF OK
func (r *_reduce) Reduce_tokens_all(middleTokens *[]Token, tokensMp *map[string]int) error {
	//reduce operation for a worker,
	// reduce a tokens list to a single list of tokens without key repetitions...reduce middle Token to final tokens
	*tokensMp = make(map[string]int)
	for _, tk := range *middleTokens {
		(*tokensMp)[tk.key]++
	}
	return nil
}
