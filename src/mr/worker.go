package mr

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type intermediateFileContents map[string][]string

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type randRead func(b []byte) (n int, err error)

// avoid collisions on the same filesystem with other mappers potentially retrying
func generateUniqueIntermediateFilename(mapTid, reduceTid int, randReader randRead) string {
	b := make([]byte, 16)
	randReader(b) // err is effectively never non-nil here, but check it in real code
	randStr := hex.EncodeToString(b)

	return fmt.Sprintf("mr-%v-%v-%v", mapTid, reduceTid, randStr)
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
			fileContents, err := readFile(task.MapArgs.File)
			if err != nil {
				fmt.Printf("failed to read map input file: %v", err.Error())
				continue // just re-loop, the coordinator should auto-detect task failure and retry the task
			}
			intermediateFiles, err := doMap(mapf, task.MapArgs.File, fileContents, task.Tid, task.MapArgs.NReduce, rand.Read)
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
		case Reduce:
			intermediateFileContents := make([]string, len(task.ReduceArgs.Files))
			for _, intermediateFilename := range task.ReduceArgs.Files {
				contents, err := readFile(intermediateFilename)
				if err != nil {
					fmt.Printf("failed to read reduce input file: %v", intermediateFilename)
					continue // just re-loop, the coordinator should auto-detect task failure and retry the task
				}
				intermediateFileContents = append(intermediateFileContents, contents)
			}

		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// fileContents contains the contents of all M intermediate files mapped to this reducer
func doReduce(reducef func(string, []string) string, fileContents []string) {

}

func doMap(mapf func(string, string) []KeyValue, inputFile, inputContents string, mapTid int, nReduce int, randReader randRead) (
	[]string, // intermediate filenames
	error,
) {
	// create nReduce intermediate files, the i-th will be for the i-th reducer
	intermediateFiles := make([]*os.File, nReduce)
	for reduceTid := range nReduce {
		fn := generateUniqueIntermediateFilename(mapTid, reduceTid, randReader)
		f, err := os.Create(fn)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		intermediateFiles[reduceTid] = f
	}

	// compute map values
	mapResults := mapf(inputFile, inputContents)
	// sort values into the nReduce intermediate files
	sortedResults := map[int]intermediateFileContents{}
	for i := range nReduce {
		sortedResults[i] = make(intermediateFileContents)
	}
	for _, kv := range mapResults {
		reduceTid := ihash(kv.Key) % nReduce
		sortedResults[reduceTid][kv.Key] = append(sortedResults[reduceTid][kv.Key], kv.Value)
	}
	// write files as JSON
	for reduceTid, contentSrc := range sortedResults {
		content, _ := json.Marshal(contentSrc)
		_, err := intermediateFiles[reduceTid].Write(content)
		if err != nil {
			return nil, err
		}
	}
	intermediateFilenames := make([]string, nReduce)
	for i, f := range intermediateFiles {
		intermediateFilenames[i] = f.Name()
	}
	return intermediateFilenames, nil
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

func readFile(filename string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	c, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(c), nil
}
