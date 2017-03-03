package main

import "os"
import "fmt"

import "strings"
import "strconv"
import "mapreduce"

import "unicode"
import "container/list"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file content. the return
// value should be a list of key/value pairs, each represented
// by a mapreduce.KeyValue.

func Map(value string) *list.List {
	words := strings.Fields(value) //parse input file into array of strings
	//fmt.Println(words)
	var a = list.New()
	var m map[string]int

	m = make(map[string]int)  //init map
	for _, b := range words { //go through words and count how many times each word appeared
		//fmt.Println(len(b))
		if len(b) > 1 && !unicode.IsLetter(rune(b[len(b)-1])) {
			b = b[0 : len(b)-1]
		}
		if !unicode.IsLetter(rune(b[0])) {
			if len(b) == 1 {
				continue
			} else {
				b = b[1:len(b)]
			}
		}
		v, ok := m[b]
		if ok {
			v += 1
			m[b] = v
		} else {
			v = 1
			m[b] = v
		}
	}

	for k, v := range m { //put everything from map into our list.
		//fmt.Println("k:", k, "v:", v)
		var kv mapreduce.KeyValue
		kv.Key = k
		kv.Value = strconv.Itoa(v)
		a.PushFront(kv)
	}
	return a
}

// called once for each key generated by Map, with a list
// of that key's string value. should return a single
// output value for that key.
func Reduce(key string, values *list.List) string {
	count := 0
	var finalValue string
	for e := values.Front(); e != nil; e = e.Next() {
		i := e.Value.(string)
		c, err := strconv.Atoi(i)
		if err == nil {
			count += c
		}
	}
	finalValue = strconv.Itoa(count)
	return finalValue
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
	//b := "-"
	//fmt.Println(unicode.IsLetter(rune(b[0])) || false)

}