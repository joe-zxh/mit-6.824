package mapreduce

import (
	"encoding/json"
	"os"
	"fmt"
)

// doReduce does the job of a reduce worker: it reads the intermediate key/value pairs (produced by the map phase) for this task,
// sorts the intermediate key/value pairs by key(相当于grouping), calls the user-defined reduce function (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvs := make(map[string][]string)

	for i:=0;i<nMap;i++ { //读取所有中间文件的key value
		reduceInputFileName := reduceName(jobName, i, reduceTaskNumber)

		mapOF, err := os.Open(reduceInputFileName)

		if err != nil {
			fmt.Println(err);
		}

		decode:=json.NewDecoder(mapOF)

		for {
			var kv KeyValue
			err = decode.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value) //grouping
		}

		mapOF.Close()
	}

	reduceOutputFileName:=mergeName(jobName, reduceTaskNumber)

	reduceOF, err := os.OpenFile(reduceOutputFileName, os.O_RDWR|os.O_CREATE, 0766)//以读写方式打开文件，如果不存在，则创建

	if err != nil {
		fmt.Println(err);
	}
	defer reduceOF.Close()

	encode:=json.NewEncoder(reduceOF)

	for k:=range  kvs {
		val:=reduceF(k, kvs[k])
		kv:=KeyValue{k, val}

		err = encode.Encode(&kv)

		if err!=nil {
			fmt.Println(err)
		}
	}

	// TODO:
	// You can find the intermediate file for this reduce task from map task number m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you will need to decode them.
	// If you chose to use JSON, you can read out multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue objects to a file named mergeName(jobName, reduceTaskNumber).
	// We require you to use JSON here because that is what the merger then combines the output from all the reduce tasks expects.
	// There is nothing "special" about JSON -- it is just the marshalling format we chose to use.
	// It will look something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
