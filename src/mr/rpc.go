package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type TaskType string
const (
	MAP 	TaskType = "MAP"
	REDUCE  TaskType = "REDUCE"
)

type Task struct {
	Type         TaskType 	// MAP, REDUCE
	Index        int
	MapInputFile string

	WorkerID string
	Deadline time.Time
}

type GetTaskArgs struct {
	WorkerID string			// Used for debugging
}

type GetTaskReply struct {
	TaskType     	TaskType // MAP or REDUCE

	NoAvaliableTask	bool
	TaskIndex    	int
	MapInputFile 	string
	MapNum       	int
	ReduceNum    	int
}

type FinishedTaskArgs struct {
	WorkerID	string
	TaskType	TaskType
	TaskIndex	int
}

type FinishedTaskReply struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
