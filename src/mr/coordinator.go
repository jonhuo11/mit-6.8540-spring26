package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"
)

const workerTimeout = 10 * time.Second

type taskEvent struct {
	attempt int // attempt == mapTask.attempt -> accept
}

type Coordinator struct {
	// Your definitions here.

	done       atomic.Bool
	nReduce    int
	inputFiles []string

	mapAcks        chan taskEvent
	mapTimeouts    chan taskEvent
	reduceAcks     chan taskEvent
	reduceTimeouts chan taskEvent
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/*
func (c *Coordinator) ReqMap(args *ReqArgs, reply *ReqReply) error {

	func timeout() {
		time.Sleep(workerTimeout)

	}
	go timeout()
}

func (c *Coordinator) AckMap(...) error {
	// map task is complete
	defer c.mapAcks <- empty{}
}
*/

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

	go c.mapReduce()

	c.server(sockname)
	return &c
}

// there will be 1 instance of this thread and it will be the only one modifying Coordinator
// no locks needed, just one atomic on c.done
func (c *Coordinator) mapReduce() {
	type mapTask struct {
		startedAt time.Time // timeout detection
		attempt   int

		file string
	}

	type reduceTask struct {
		startedAt time.Time

		tid uint // range from [0, nReduce)
	}

	defer c.done.Store(false)

	// spawn one map task per input file
	nMapTasks := len(c.inputFiles)
	mapTasks := make([]mapTask, nMapTasks)
	incompleteMapTasks := nMapTasks
	for i, inputFile := range c.inputFiles {
		mapTasks[i].done = false
		mapTasks[i].file = inputFile
	}
	// wait for all map tasks to be claimed and completed with ACKs
	for incompleteMapTasks > 0 {

	}

	// spawn nReduce number of reduce tasks
	// wait for all reduce tasks to be claimed and completed with ACKs
}
