package mapreduce

import "container/list"
import "fmt"

//import "reflect"
import "strconv"

//import "log"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address) //report worker shutdown error.
		} else {
			l.PushBack(reply.Njobs) //collect all the works done by workers.
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	nMap := mr.nMap
	nReduce := mr.nReduce

	go func() {
		close(mr.registerChannel)
	}()
	count := 0
	for worker := range mr.registerChannel {
		var info WorkerInfo
		info.address = worker
		mr.Workers["worker"+strconv.Itoa(count)] = &info
		count++
	}
	for i := 0; i < nMap; i++ {
		for worker := range mr.Workers {
			addr := (*mr.Workers[worker]).address
			args := &DoJobArgs{mr.file, Map, i, nReduce}
			var reply DoJobReply

			ok := call(addr, "Worker.DoJob", args, &reply)
			if ok == false {
				continue // go back to the worker for loop try next worker.
			}
		}

	}
	//run reduce
	for i := 0; i < nReduce; i++ {
		for worker := range mr.Workers {
			addr := (*mr.Workers[worker]).address
			args := &DoJobArgs{mr.file, Reduce, i, nMap}
			var reply DoJobReply

			ok := call(addr, "Worker.DoJob", args, &reply)
			if ok == false {
				continue
			}
		}

	}

	return mr.KillWorkers() //this will return the collected works
}
