package mapreduce

import (
        //"io/ioutil"
        "os"
        "log"
        //"strconv"
        "encoding/json"
        "sort"
	//"fmt"
)

//import s "strings"



func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	
	//dir_list, err := ioutil.ReadDir(".")
	//if err != nil {
	//	log.Fatal(err)
	//}
	mapFile := make(map[string][]string)
	var kv KeyValue

	//for _, v := range dir_list {  //or use reduceName(jobName, 0, reduceTask)
    for i:=0; i<nMap; i++ {
		intermFile := reduceName(jobName, i, reduceTask)
	        
		resFile, _ := os.Open(intermFile)
		dec := json.NewDecoder(resFile)
		for {
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			valList, ok := mapFile[kv.Key]
			if ok {
				mapFile[kv.Key] = append(valList, kv.Value)
			} else {
				mapFile[kv.Key] = []string{kv.Value}
			}
		}	
		for key, _ := range(mapFile) {
			sort.Strings(mapFile[key])
		}
	}
	////ReduceFunc(key string, values []string) string {
	////TODO: sort
	////mergeName(jobName string, reduceTask int) string 
	
	mergeFile, _ := os.Create(mergeName(jobName, reduceTask))
	mergeFileEncoder := json.NewEncoder(mergeFile)
	
	for k, v := range(mapFile) {
		reducedValue := reduceF(k, v)
		kv.Key = k
		kv.Value = reducedValue
		err := mergeFileEncoder.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	mergeFile.Close()
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
