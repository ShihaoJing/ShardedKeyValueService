package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const debug = 1

// DPrintf helper function to print logs
func DPrintf(level int, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if debug >= level {
		log.Printf(format, a...)
	}
	return
}

// Operation types
const (
	Put    = "Put"
	Append = "Append"
	Get    = "Get"
)

// Op Op struct
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
}

// Result Result struct
type Result struct {
	err   Err
	value string // value for GET method
}

// KVServer KVServer
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap    map[string]string
	resultCh map[int]chan Result
}

func (kv *KVServer) waitForCommitedOp() {
	timer := time.NewTicker(100 * time.Millisecond)
	for {
		if kv.killed() {
			DPrintf(1, "KV[%d] got killed !!!\n", kv.me)
			return
		}
		select {
		case <-timer.C:
			DPrintf(2, "KV[%d]: timeout while waiting for commited op.\n", kv.me)
		case msg := <-kv.applyCh:
			DPrintf(1, "KV[%d]: received a commited op: %+v\n", kv.me, msg)
			op := msg.Command.(Op)
			err, val := kv.executeOp(op)
			result := Result{err, val}
			kv.mu.Lock()
			if resultChan, ok := kv.resultCh[msg.CommandIndex]; ok == true {
				resultChan <- result
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) executeOp(op Op) (Err, string) {
	var err Err
	var val string
	switch op.OpType {
	case Put:
		DPrintf(1, "KV[%d]: executing Put op: %+v\n", kv.me, op)
		kv.kvMap[op.Key] = op.Value
		err = OK
		val = ""
	case Append:
		DPrintf(1, "KV[%d]: executing Append op: %+v\n", kv.me, op)
		kv.kvMap[op.Key] = kv.kvMap[op.Key] + op.Value
		err = OK
		val = ""
	case Get:
		DPrintf(1, "KV[%d]: executing Get op: %+v\n", kv.me, op)
		v, _ := kv.kvMap[op.Key]
		err = OK
		val = v

		// if ok == false {
		// 	err = ErrNoKey
		// 	val = ""
		// } else {
		// 	err = OK
		// 	val = v
		// }
	}
	return err, val
}

// Get RPC handler of Get
func (kv *KVServer) Get(args *GetRequest, reply *GetReply) {
	// Your code here.
	DPrintf(1, "KV[%d] received Get: %+v\n", kv.me, args)
	op := Op{Get, args.Key, ""}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	// Revisit in case of failure
	kv.mu.Lock()
	kv.resultCh[index] = make(chan Result)
	kv.mu.Unlock()

	timer := time.NewTimer(300 * time.Millisecond)
	select {
	case result := <-kv.resultCh[index]:
		reply.Err = result.err
		reply.Value = result.value
	case <-timer.C:
		reply.Err = ErrTimeOut
		DPrintf(1, "KV[%d]: Get timeout!\n", kv.me)
	}
	return
}

// PutAppend RPC handler of PutAppend
func (kv *KVServer) PutAppend(args *PutAppendRequest, reply *PutAppendReply) {
	DPrintf(1, "KV[%d] received PutAppend: %+v\n", kv.me, args)
	op := Op{args.Op, args.Key, args.Value}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	// Revisit in case of failure
	kv.mu.Lock()
	kv.resultCh[index] = make(chan Result)
	kv.mu.Unlock()

	timer := time.NewTimer(300 * time.Millisecond)
	select {
	case result := <-kv.resultCh[index]:
		reply.Err = result.err
	case <-timer.C:
		reply.Err = ErrTimeOut
		DPrintf(1, "KV[%d]: PutAppend timeout!\n", kv.me)
	}
	return
}

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.kvMap = make(map[string]string)
	kv.resultCh = make(map[int]chan Result)
	go kv.waitForCommitedOp()

	return kv
}
