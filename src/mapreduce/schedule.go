package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var workerRPCAddr string
	var wg sync.WaitGroup
	cnt := 0
	fmt.Println("sfdfsi%v", ntasks)
	for ; cnt<ntasks; cnt++ {
		
		wg.Add(1)
		workerRPCAddr = <- registerChan
		fmt.Println("%s kk", workerRPCAddr)
		tmp := DoTaskArgs{jobName, mapFiles[cnt], phase, cnt, n_other}
		go func(workerRPCAddr string) {
			//for {
				//workerRPCAddr = <- registerChan //put inside, or pass a value. should not use closure, may cause error
				//fmt.Println("%s kk", workerRPCAddr)
				call(workerRPCAddr, "Worker.DoTask", &tmp, nil)
				defer func(workerRPCAddr string) {
					registerChan <- workerRPCAddr
				}(workerRPCAddr)
				//if success {
				//	break
				//}
			//}
			wg.Done()
		}(workerRPCAddr)
		fmt.Println("hahahahahah")
		//registerChan <- workerRPCAddr
		
	}
	fmt.Println("sfdfs......")
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
