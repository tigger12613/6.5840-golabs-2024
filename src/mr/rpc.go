package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskType string

const (
	NoTask   TaskType = "noTask"
	HaveTask TaskType = "haveTask"
)

// map task
type Task struct {
	// Coordinator give the filename for map task
	InputFile string
	// Worker give back all mr-X-Y filenames to Coordinator
	OutputFile []string
	TaskType   TaskType
	// The id of task
	SerialNum int
	// Total reduce number, use for ihash()%NReduce
	NReduce int
}
type ReduceTask struct {
	// Give all mr-X-Y produce by map
	InputFiles []string
	// The id of task
	SerialNum int
	TaskType  TaskType
	// Total reduce number, use for ihash()%NReduce
	NReduce int
}
type IsFinishedReply struct {
	IsFinished bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
