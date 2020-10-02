package kvraft

// Operation status
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
)

// Err canonical error type
type Err string

// PutAppendRequest RPC request of Put or Append
type PutAppendRequest struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

// PutAppendReply RPC reply
type PutAppendReply struct {
	Err Err
}

// GetRequest RPC request of Get
type GetRequest struct {
	Key string
	// You'll have to add definitions here.
}

// GetReply RPC reply of Get
type GetReply struct {
	Err   Err
	Value string
}
