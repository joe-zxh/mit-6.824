package mapreduce

import (
	"hash/fnv"
	"fmt"
	"os"
	"io/ioutil"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	f, err:=os.Open(inFile)
	if (err!=nil) {
		fmt.Print(err)
	}
	defer f.Close()

	b, err:=ioutil.ReadAll(f)

	if (err!=nil) {
		fmt.Print(err)
	}

	mapOutput:=mapF(inFile, string(b)) //返回的是[]KeyValue, key是key，value是空的""。

	var mapOutputFile []*os.File
	var enc []*json.Encoder

	for i:=0;i<nReduce;i++ {
		fname:=reduceName(jobName, mapTaskNumber, i)
		mapOF, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE, 0766)//以读写方式打开文件，如果不存在，则创建
		if err != nil {
			fmt.Println(err);
		}

		mapOutputFile = append(mapOutputFile, mapOF)
		defer mapOutputFile[i].Close()

		encode:=json.NewEncoder(mapOutputFile[i])
		enc = append(enc, encode)
	}

	for _, kv := range (mapOutput) {
		reduceId:=ihash(kv.Key)%(uint32(nReduce))
		err = enc[reduceId].Encode(&kv)

		if err!=nil {
			fmt.Println(err)
		}
	}

	fmt.Println("Map Finish!")

	// TODO:
	// You can find the filename for this map task's input to reduce task number r using reduceName(jobName, mapTaskNumber, r).
	// The ihash function (given below doMap) should be used to decide which (reduce) file a given key belongs into. 就是说，对于每个key，它应该被哪一个reducetask处理 是 通过hash一下在modreduceNum
	//
	// The intermediate output of a map task is stored in the file system as multiple files whose name indicates which map task produced them, as well as which reduce task they are for.
	// Coming up with a scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the other end can correctly reconstruct is JSON.
	// You are not required to use JSON, but as the output of the reduce tasks *must* be JSON, familiarizing yourself with it here may prove useful.
	// You can write out a data structure as a JSON string to a file using the commented code below.
	// The corresponding decoding functions can be found in common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
