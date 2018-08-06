package mapreduce

import (
	"fmt"
	"strconv"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup

	for i:=0;i<ntasks; i++{
		//mr.Mutex.Lock()
		wg.Add(1)
		//mr.Mutex.Unlock()
		go mr.callWorkerDoTask(i, phase, nios, &wg)
	}

	wg.Wait()

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master)callWorkerDoTask(taskNumber int, phase jobPhase, nios int, wg *sync.WaitGroup) {
	idleWorker:= <-mr.registerChannel

	var fileName string

	if(phase==mapPhase){
		fileName=fmt.Sprintf("824-mrinput-%v.txt", strconv.Itoa(taskNumber))
	}else{ //reducePhase 不需要指定输入文件名
		fileName=""
	}

	args:=DoTaskArgs{mr.jobName, fileName, phase, taskNumber, nios}

	ok := call(idleWorker, "Worker.DoTask", args, new(struct{}))



	if ok == false {
		fmt.Printf("Register: RPC %s DoTask error, %v \n", idleWorker, args)
		mr.callWorkerDoTask(taskNumber, phase, nios, wg)
	} else {
		//mr.Mutex.Lock()
		wg.Done()
		//mr.Mutex.Unlock()

		mr.registerChannel <-idleWorker //告诉master, 这个worker idle了 !!!这一行一定要放在最后，不然最后2个任务完成后,worker无法释放
	}
}

