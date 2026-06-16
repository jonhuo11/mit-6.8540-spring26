package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const workerTimeout = 10 * time.Second
const reqTaskTimeout = 1 * time.Second

type mapTask struct {
	tid               int
	file              string
	done              bool
	intermediateFiles []struct {
		reducerTid int
		file       string
	}
}

type reduceTask struct{}

type Coordinator struct {
	// Your definitions here.

	done       atomic.Bool
	nReduce    int
	inputFiles []string

	mu sync.Mutex
	// mapper outputs are deterministic + identical for the same input block, so it doesn't matter who completes the task
	// the first completion always marks the state as complete, and further completions (from timed out workers replying) are ignored
	mapTaskStates   []mapTask
	mapQueue        chan mapTask
	completionQueue chan CompleteTaskArgs
	reduceQueue     chan reduceTask
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	// select a task from the queue
	// start a timeout goroutine which re-adds the task to the queue after a timeout
	return nil
}

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
	return c.done.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.done.Store(false)
	c.nReduce = nReduce
	c.inputFiles = files
	c.mapQueue = make(chan mapTask, 128)
	c.reduceQueue = make(chan reduceTask, 128)
	c.completionQueue = make(chan CompleteTaskArgs, 128)

	go c.mapReduce()

	c.server(sockname)
	return &c
}

func (c *Coordinator) mapReduce() {
	defer c.done.Store(false)

	// spawn one map task per input file
	nMapTasks := len(c.inputFiles)
	incompleteMapTasks := nMapTasks
	for tid, inputFile := range c.inputFiles {
		c.mapQueue <- mapTask{
			tid:  tid,
			file: inputFile,
			done: false,
			intermediateFiles: make([]struct {
				reducerTid int
				file       string
			}, c.nReduce),
		}
	}
	// wait for all map tasks to be claimed and completed with ACKs
	// even if the map queue is empty we can go
	for incompleteMapTasks > 0 {
		taskResult := <-c.completionQueue // TODO: timeout here for total failure if no progress is made after some minutes
		if taskResult.Type == Map {
			if taskResult.Tid < 0 || taskResult.Tid >= len(c.mapTaskStates) {
				continue
			}
			if len(taskResult.MapOut.IntermediateFiles) != c.nReduce {
				return // TODO: requeue the map task, it failed
			}

			func() {
				c.mu.Lock()
				defer c.mu.Unlock()

				tState := &c.mapTaskStates[taskResult.Tid]
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
		} // ignore other types for this phase
	}

	// spawn nReduce number of reduce tasks

	// wait for all reduce tasks to be claimed and completed with ACKs
}
