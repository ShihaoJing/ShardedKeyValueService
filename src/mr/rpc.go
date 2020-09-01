package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskType struct {
	ID         int
	Type       int // 0 map, 1 reduce
	NumReduce  int
	InputFiles []string
}

type FetchTaskRequest struct {
	WorkerID int
}

type FetchTaskResponse struct {
	Status int // 0 success 1 no task available 2 all tasks finished
	Task   TaskType
}

type FetchWorkerIDRequest struct {
}

type FetchWorkerIDResponse struct {
	WorkerID int
}

type ReportTaskRequest struct {
	WorkerID int
	Action   int // 0 fetch task, 1 report completion
}

type ReportTaskResponse struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
