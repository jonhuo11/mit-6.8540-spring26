package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ReqTaskArgs struct {
}

type TaskType int

const (
	TaskTypeMap    TaskType = 0
	TaskTypeReduce TaskType = 1
	TaskTypeSleep  TaskType = 2
)

type ReqTaskReply struct {
	Type       TaskType
	Tid        int
	MapArgs    ReqTaskReplyMapArgs
	ReduceArgs ReqTaskReplyReduceArgs
}

type ReqTaskReplyMapArgs struct {
	File    string
	NReduce int // info to help hash
}
type ReqTaskReplyReduceArgs struct { // the Tid will be the reducer task
	Files []string // intermediate files assigned to this reducer
}

type IntermediateFile struct {
	ReducerTid int
	File       string // filename does not matter, must be unique per run to prevent collisions
}

type CompleteTaskArgs struct {
	Type   TaskType
	Tid    int
	MapOut struct {
		IntermediateFiles []IntermediateFile // there should be nReduce files, one for each reducer
	}
	ReduceOut struct {
		File string // filename of completed task
	}
}

func (cta *CompleteTaskArgs) deepCopy() CompleteTaskArgs {
	if cta == nil {
		return CompleteTaskArgs{}
	}
	copyCta := CompleteTaskArgs{
		Type: cta.Type,
		Tid:  cta.Tid,
	}
	// Deep copy MapOut.IntermediateFiles
	copyCta.MapOut.IntermediateFiles = make([]IntermediateFile, len(cta.MapOut.IntermediateFiles))
	copy(copyCta.MapOut.IntermediateFiles, cta.MapOut.IntermediateFiles)

	// (If ReduceOut gets fields later, deep copy them here)
	return copyCta
}

type CompleteTaskReply struct{}
