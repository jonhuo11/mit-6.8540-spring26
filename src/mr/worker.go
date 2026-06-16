package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// avoid collisions on the same filesystem with other mappers potentially retrying
func generateUniqueIntermediateFilename() string {
	return ""
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		task, err := CallReqTask(&ReqTaskArgs{})
		if err != nil {
			// assume the coordinator is done
			fmt.Printf("coordinator stopped responding, exiting")
			return
		}

		switch task.Type { // map or reduce w args
		case Map:
			intermediateFiles, err := doMap(mapf, task.MapArgs.File, task.MapArgs.NReduce)
			if err != nil {
				continue // just re-loop, the coordinator should auto-detect task failure and retry the task
			}
			convIntermediateFiles := make([]IntermediateFile, len(intermediateFiles))
			for reducerTid, filename := range intermediateFiles {
				convIntermediateFiles = append(convIntermediateFiles, IntermediateFile{
					ReducerTid: reducerTid,
					File:       filename,
				})
			}
			_, err = CallCompleteTask(&CompleteTaskArgs{
				Type: Map,
				Tid:  task.Tid,
				MapOut: struct{ IntermediateFiles []IntermediateFile }{
					IntermediateFiles: convIntermediateFiles,
				},
			})
			if err != nil {
				// assume the coordinator is done
				fmt.Printf("coordinator stopped responding, exiting")
				return
			}
		case Reduce: // TODO: implement this
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// TODO: implement
func doMap(mapf func(string, string) []KeyValue, file string, nReduce int) (
	[]string, // intermediate filenames
	error,
) {
	// create nReduce intermediate files, the i-th will be for the i-th reducer
	// compute map values
	// sort values into the nReduce intermediate files

	return nil, nil
}

func CallReqTask(args *ReqTaskArgs) (*ReqTaskReply, error) {
	reply := ReqTaskReply{}
	if ok := call("Coordinator.ReqTask", args, &reply); !ok {
		return nil, errors.New("")
	}
	return &reply, nil
}

// TODO: implement
func CallCompleteTask(args *CompleteTaskArgs) (*CompleteTaskReply, error) {
	return nil, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
