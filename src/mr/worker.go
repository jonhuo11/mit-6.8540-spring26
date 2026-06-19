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
	randv2 "math/rand/v2"
	"net/rpc"
	"os"
	"time"
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
		fmt.Println("worker requesting task...")
		task, err := CallReqTask(&ReqTaskArgs{})
		if err != nil {
			// assume the coordinator is done
			fmt.Printf("coordinator stopped responding, exiting\n")
			return
		}
		fmt.Printf("worker recv'd task type %v\n", task.Type)

		switch task.Type { // map or reduce w args
		case TaskTypeMap:
			fmt.Printf("worker starting map task %v: %v\n", task.Tid, task.MapArgs.File)
			fileContents, err := readFile(task.MapArgs.File)
			if err != nil {
				fmt.Printf("failed to read map input file %v: %v\n", task.MapArgs.File, err.Error())
				continue // just re-loop, the coordinator should auto-detect task failure and retry the task
			}
			intermediateFiles, err := doMap(mapf, task.MapArgs.File, fileContents, task.Tid, task.MapArgs.NReduce, rand.Read)
			if err != nil {
				fmt.Printf("worker failed to execute map task %v: %v\n", task.Tid, err.Error())
				continue // just re-loop, the coordinator should auto-detect task failure and retry the task
			}
			convIntermediateFiles := []IntermediateFile{}
			for reducerTid, filename := range intermediateFiles {
				convIntermediateFiles = append(convIntermediateFiles, IntermediateFile{
					ReducerTid: reducerTid,
					File:       filename,
				})
			}
			_, err = CallCompleteTask(&CompleteTaskArgs{
				Type: TaskTypeMap,
				Tid:  task.Tid,
				MapOut: struct{ IntermediateFiles []IntermediateFile }{
					IntermediateFiles: convIntermediateFiles,
				},
			})
			if err != nil {
				// assume the coordinator is done
				fmt.Printf("coordinator stopped responding, exiting\n")
				return
			}
			fmt.Printf("worker finished map task %v\n", task.Tid)
		case TaskTypeReduce:
			fmt.Printf("worker starting reduce task %v, intermediate files: %v\n", task.Tid, task.ReduceArgs.Files)
			intermediateFileContents := []string{}
			for _, intermediateFilename := range task.ReduceArgs.Files {
				contents, err := readFile(intermediateFilename)
				if err != nil {
					fmt.Printf("failed to read reduce input file: %v\n", intermediateFilename)
					continue // just re-loop, the coordinator should auto-detect task failure and retry the task
				}
				intermediateFileContents = append(intermediateFileContents, contents)
			}
			err := doReduce(reducef, intermediateFileContents, task.Tid)
			if err != nil {
				fmt.Printf("worker failed to execute reduce task %v: %v\n", task.Tid, err.Error())
				continue
			}

			_, err = CallCompleteTask(&CompleteTaskArgs{
				Type: TaskTypeReduce,
				Tid:  task.Tid,
			})
			if err != nil {
				// assume the coordinator is done
				fmt.Printf("coordinator stopped responding, exiting\n")
				return
			}
			fmt.Printf("worker finished reduce task %v\n", task.Tid)
		case TaskTypeSleep:
			time.Sleep(time.Second * time.Duration(randv2.IntN(5)))
		}
	}
}

// fileContents contains the contents of all M intermediate files mapped to this reducer
func doReduce(reducef func(string, []string) string, fileContents []string, reducerTid int) error {
	reductionMap := map[string][]string{}
	for _, fc := range fileContents {
		tmp := map[string][]string{}
		if err := json.Unmarshal([]byte(fc), &tmp); err != nil {
			return err
		}
		for k, values := range tmp {
			reductionMap[k] = append(reductionMap[k], values...)
		}
	}
	fmt.Printf("Reducer %v will process %d keys aggregated over the intermediate files\n", reducerTid, len(reductionMap))
	outputFile, err := os.Create(fmt.Sprintf("mr-out-%v", reducerTid))
	if err != nil {
		return err
	}
	defer outputFile.Close()
	for k, values := range reductionMap {
		out := reducef(k, values)
		fmt.Fprintf(outputFile, "%v %v\n", k, out)
	}
	return nil
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
		return nil, errors.New("failed to call ReqTask on coordinator")
	}
	return &reply, nil
}

func CallCompleteTask(args *CompleteTaskArgs) (*CompleteTaskReply, error) {
	reply := CompleteTaskReply{}
	if ok := call("Coordinator.CompleteTask", args, &reply); !ok {
		return nil, errors.New("failed to call CompleteTask on coordinator")
	}
	return &reply, nil
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
	log.Printf("%d: call failed err %v\n", os.Getpid(), err)
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
