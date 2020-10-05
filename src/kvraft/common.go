package kvraft

import "log"

const debug = 0

// DPrintf helper function to print logs
func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug >= level {
		log.Printf(format, a...)
	}
	return
}

// Operation status
const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrTimeOut          = "ErrTimeOut"
	ErrDuplicateRequest = "ErrDuplicateRequest"
	ErrUnmatchedOp      = "ErrUnmatchedOp"
)

// Err canonical error type
type Err string

// PutAppendRequest RPC request of Put or Append
type PutAppendRequest struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientID  int64
	RequestID int64
}

// PutAppendReply RPC reply
type PutAppendReply struct {
	Err Err
}

// GetRequest RPC request of Get
type GetRequest struct {
	Key       string
	ClientID  int64
	RequestID int64
}

// GetReply RPC reply of Get
type GetReply struct {
	Err   Err
	Value string
}
