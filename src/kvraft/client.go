package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

// Clerk clert struct
type Clerk struct {
	me      int64
	servers []*labrpc.ClientEnd
	leader  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 20)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk make a cleark
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.me = nrand()
	ck.leader = 0
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	value := ""
	request := GetRequest{key, ck.me, nrand()}
	for ; ; ck.leader = ck.leader % len(ck.servers) {
		reply := GetReply{}
		DPrintf(1, "Clerk[%d] sending Get to server [%d]: %+v\n", ck.me, ck.leader, request)
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &request, &reply); ok == true {
			DPrintf(1, "Clerk[%d] received GetReply from server[%d]: %+v\n", ck.me, ck.leader, reply)
			if reply.Err == OK || reply.Err == ErrNoKey {
				value = reply.Value
				break
			}
		} else {
			DPrintf(1, "Clerk[%d]: Get timeout when waiting RPC response from server [%d]: %+v\n", ck.me, ck.leader, request)
		}
		ck.leader++
		time.Sleep(10 * time.Millisecond)
	}
	return value
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	request := PutAppendRequest{key, value, op, ck.me, nrand()}
	for {
		reply := PutAppendReply{}
		DPrintf(1, "Clerk[%d] sending PutAppend to server[%d]: %+v\n", ck.me, ck.leader, request)
		if ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &request, &reply); ok == true {
			DPrintf(1, "Clerk[%d] received PutAppendReply from server [%d]: %+v\n", ck.me, ck.leader, reply)
			if reply.Err == OK {
				break
			}
		} else {
			DPrintf(1, "Clerk[%d]: PutAppend timeout when waiting RPC response from server [%d]: %+v\n", ck.me, ck.leader, request)
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// Put put method
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append append method
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
