package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const workerTimeout = 10 * time.Second
const reqTaskTimeout = 3 * time.Second

type task struct {
	tid      int
	taskType coordinatorState
}

type mapTaskState struct {
	tid               int
	done              bool
	inputFile         string
	intermediateFiles []struct {
		reducerTid int
		file       string
	}
}

type reduceTaskState struct {
	tid               int
	done              bool
	intermediateFiles []string
}

type coordinatorState uint

const (
	coordinatorStateMap coordinatorState = iota
	coordinatorStateReduce
	coordinatorStateDone
)

type Coordinator struct {
	// Your definitions here.

	nReduce    int
	inputFiles []string

	mu sync.RWMutex
	// mapper outputs are deterministic + identical for the same input block, so it doesn't matter who completes the task
	// the first completion always marks the state as complete, and further completions (from timed out workers replying) are ignored
	mapTaskState    []mapTaskState
	reduceTaskState []reduceTaskState
	taskQueue       chan task
	completionQueue chan CompleteTaskArgs
	coordinatorState
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	// select a task from the queue
	select {
	case t := <-c.taskQueue:
		c.mu.RLock()
		if t.taskType != c.coordinatorState { // skip, out of sync task
			c.mu.RUnlock()
			fmt.Printf("task type mismatch: taskType=%v coordinatorState=%v\n", t.taskType, c.coordinatorState)
			reply.Type = TaskTypeSleep
			return nil
		}

		reply.Tid = t.tid
		switch t.taskType {
		case coordinatorStateMap:
			mt := &c.mapTaskState[t.tid]
			reply.Type = TaskTypeMap
			reply.MapArgs = ReqTaskReplyMapArgs{
				File:    mt.inputFile,
				NReduce: c.nReduce,
			}
		case coordinatorStateReduce:
			rt := &c.reduceTaskState[t.tid]
			reply.Type = TaskTypeReduce
			reply.ReduceArgs.Files = make([]string, len(rt.intermediateFiles))
			copy(reply.ReduceArgs.Files, rt.intermediateFiles)
		default:
			reply.Type = TaskTypeSleep
			c.mu.RUnlock()
			fmt.Printf("unknown task type %v queued by coordinator", t.taskType)
			return nil
		}
		c.mu.RUnlock()

		// start a timeout goroutine which re-adds the task to the queue after a timeout, if its still valid
		go func() {
			time.Sleep(workerTimeout)
			c.mu.RLock()
			defer c.mu.RUnlock()

			var taskDone bool
			switch t.taskType {
			case coordinatorStateMap:
				taskDone = c.mapTaskState[t.tid].done
			case coordinatorStateReduce:
				taskDone = c.reduceTaskState[t.tid].done
			default:
				taskDone = true // Unknown or irrelevant task types are considered done
			}
			if t.taskType != c.coordinatorState || taskDone {
				return
			}

			c.taskQueue <- t
		}()
		return nil
	case <-time.After(reqTaskTimeout):
		fmt.Println("task req timed out")
		reply.Type = TaskTypeSleep
		return nil
	}
}

// late workers might respond with map task completion packets when coordinator is already reducing,
// but those will be filtered out by the main loop
func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.completionQueue <- args.deepCopy()

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.coordinatorState == coordinatorStateDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.coordinatorState = coordinatorStateMap
	c.nReduce = nReduce
	c.inputFiles = files
	c.mapTaskState = make([]mapTaskState, len(files))
	c.reduceTaskState = make([]reduceTaskState, nReduce)
	c.taskQueue = make(chan task, 128)
	c.completionQueue = make(chan CompleteTaskArgs, 128)

	for tid, inputFile := range c.inputFiles { // setup map states + queue initial tasks
		c.mapTaskState[tid] = mapTaskState{
			tid:       tid,
			done:      false,
			inputFile: inputFile,
			intermediateFiles: make([]struct {
				reducerTid int
				file       string
			}, c.nReduce),
		}
		c.taskQueue <- task{
			tid:      tid,
			taskType: coordinatorStateMap,
		}
	}

	for tid := range nReduce { // setup reduce states
		c.reduceTaskState[tid].tid = tid
		c.reduceTaskState[tid].done = false
		c.reduceTaskState[tid].intermediateFiles = []string{}
	}

	go c.mapReduce()

	c.server(sockname)
	return &c
}

func (c *Coordinator) mapReduce() {
	fmt.Println("starting map phase...")
	c.mu.RLock()
	nMapTasks := len(c.inputFiles)
	incompleteMapTasks := nMapTasks
	nReduce := c.nReduce
	c.mu.RUnlock()

	// wait for all map tasks to be claimed and completed with ACKs
	// even if the map queue is empty we can go
	for incompleteMapTasks > 0 {
		taskResult := <-c.completionQueue // TODO: timeout here for total failure if no progress is made after some minutes
		fmt.Printf("map task %v completion ACK received\n", taskResult.Tid)
		if taskResult.Type != TaskTypeMap {
			continue
		}
		if taskResult.Tid < 0 || taskResult.Tid >= nMapTasks {
			continue
		}
		if len(taskResult.MapOut.IntermediateFiles) != nReduce {
			fmt.Printf("%v %v", len(taskResult.MapOut.IntermediateFiles), nReduce)
			c.taskQueue <- task{
				tid:      taskResult.Tid,
				taskType: coordinatorStateMap,
			}
			continue
		}

		func() {
			c.mu.Lock()
			defer c.mu.Unlock()

			tState := &c.mapTaskState[taskResult.Tid]
			if tState.done { // enforce exactly-once semantics for completion
				return
			}
			for i, v := range taskResult.MapOut.IntermediateFiles {
				f := &tState.intermediateFiles[i]
				f.file = v.File
				f.reducerTid = v.ReducerTid
			}
			tState.done = true
			incompleteMapTasks--
		}()

		fmt.Printf("map task %v completion ACK validated\n", taskResult.Tid)
	}

	// spawn nReduce number of reduce tasks
	c.mu.Lock()
	incompleteReduceTasks := nReduce
	for tid := range nReduce {
		c.taskQueue <- task{
			tid:      tid,
			taskType: coordinatorStateReduce,
		}
	}
	for _, mts := range c.mapTaskState {
		for _, imFile := range mts.intermediateFiles {
			c.reduceTaskState[imFile.reducerTid].intermediateFiles = append(c.reduceTaskState[imFile.reducerTid].intermediateFiles, imFile.file)
		}
	}
	c.coordinatorState = coordinatorStateReduce
	c.mu.Unlock()
	fmt.Printf("starting reduce phase with %v tasks\n", incompleteReduceTasks)

	// wait for all reduce tasks to be claimed and completed with ACKs
	for incompleteReduceTasks > 0 {
		taskResult := <-c.completionQueue // TODO: timeout here for total failure if no progress is made after some minutes
		fmt.Printf("reduce task %v completion ACK received\n", taskResult.Tid)
		if taskResult.Type != TaskTypeReduce {
			continue
		}
		if taskResult.Tid < 0 || taskResult.Tid >= nReduce {
			continue
		}
		func() {
			c.mu.Lock()
			defer c.mu.Unlock()

			tState := &c.reduceTaskState[taskResult.Tid]
			if tState.done { // enforce exactly-once semantics for completion
				return
			}
			tState.done = true
			incompleteReduceTasks--
		}()
		fmt.Printf("reduce task %v completion ACK validated, final output in %v\n", taskResult.Tid, taskResult.ReduceOut.File)
	}

	c.mu.Lock()
	c.coordinatorState = coordinatorStateDone
	c.mu.Unlock()
	fmt.Printf("done mapReduce\n")
}
