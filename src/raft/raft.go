package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// TimeOutMillSeconds time out duration
const TimeOutMillSeconds = 150

// NilCandidateID nil candidate id
const NilCandidateID = -1

const (
	// Follower current server is a follower
	Follower = iota
	// Candidate current server is a candidate
	Candidate
	// Leader current server is a leader
	Leader
)

// import "bytes"
// import "../labgob"

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log Command log
type Log struct {
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	state       int
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state on all servers
	lastLogIndex int
	commitIndex  int
	lastApplied  int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	// In election
	electionTimer *time.Ticker
	votesReceived int
	heartbeat     chan bool

	applyCh chan ApplyMsg
}

func randomElectionTimeout() int {
	rand.Seed(time.Now().UnixNano())
	min := 300
	max := 400
	return rand.Intn(max-min) + min
}

func (rf *Raft) init() {
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NilCandidateID
	rf.log = make([]Log, 100)
	rf.log[0] = Log{0, "sentinel_entry"}
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesReceived = 0
	rf.heartbeat = make(chan bool)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimer = time.NewTicker(time.Duration(randomElectionTimeout()) * time.Millisecond)
	go rf.electionTiming()
	go rf.applyCommands()
}

func (rf *Raft) applyCommands() {
	for {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			DPrintf(1, "Server [%d] commiting log at index[%d]: %+v", rf.me, rf.lastApplied, rf.log[rf.lastApplied])
			rf.applyCh <- ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) electionTiming() {
	for {
		if rf.killed() {
			DPrintf(1, "Peer [%d] got killed !!!\n", rf.me)
			return
		}
		select {
		case <-rf.heartbeat:
			DPrintf(3, "Peer [%d] received heatbeat. Reset Timer.\n", rf.me)
			rf.electionTimer = time.NewTicker(time.Duration(randomElectionTimeout()) * time.Millisecond)
		case <-rf.electionTimer.C:
			DPrintf(3, "Peer [%d] election timeout\n", rf.me)
			rf.mu.Lock()
			if rf.state != Leader {
				DPrintf(1, "Peer [%d] converts to candidate at new term [%d]\n", rf.me, rf.currentTerm+1)
				rf.state = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.votesReceived = 1
				req := &VoteRequest{rf.currentTerm, rf.me, rf.lastLogIndex, rf.log[rf.lastLogIndex].Term}
				for index := range rf.peers {
					if index == rf.me {
						continue
					}
					go rf.requestVote(index, req)
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for {
		if rf.killed() {
			DPrintf(1, "Peer [%d] got killed !!!\n", rf.me)
			return
		}
		rf.mu.Lock()
		if rf.state == Leader {
			DPrintf(3, "Leader [%d] starts sending heartbeats\n", rf.me)
			req := &AppendEntriesRequest{rf.me, rf.currentTerm, rf.lastLogIndex, rf.log[rf.lastLogIndex].Term, []Log{}, rf.commitIndex}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				DPrintf(3, "Leader [%d] sending heartbeats to [%d]\n", rf.me, i)
				resp := &AppendEntriesResponse{}
				go rf.sendAppendEntries(i, req, resp)
			}
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// This function has to run in parallel since slow responders may trigger
// live server's election timeouts and a leader may never appear.
func (rf *Raft) requestVote(server int, req *VoteRequest) {
	resp := &VoteResponse{}
	DPrintf(1, "Candidate %d sending vote request to peer %d\n", rf.me, server)
	if rf.sendRequestVote(server, req, resp) {
		DPrintf(1, "Candidate %d received response from peer %d: %+v\n", rf.me, server, resp)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if resp.VoteGranted {
			rf.votesReceived++
			DPrintf(1, "Candidate %d votes received: %d\n", rf.me, rf.votesReceived)
			if rf.state == Candidate && rf.votesReceived > len(rf.peers)/2 {
				rf.state = Leader
				DPrintf(1, "!!!Candidate [%d] converts to leader!!!\n", rf.me)
				go rf.sendHeartBeat()
			}
		}
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// VoteRequest RPC request.
type VoteRequest struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// VoteResponse RPC response.
type VoteResponse struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *VoteRequest, reply *VoteResponse) {
	// // Your code here (2A, 2B).
	// DPrintf("Peer %d received request: %+v\n", rf.me, args)
	// DPrintf("Peer %d state: voted for[%d], term[%d], commit_index[%d], last_log_term[%d]\n", rf.me, rf.votedFor, rf.currentTerm, rf.commitIndex, rf.log[rf.commitIndex].term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm >= args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.lastLogIndex > args.LastLogIndex || rf.log[rf.lastLogIndex].Term > args.LastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		rf.heartbeat <- true
		rf.state = Follower
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *VoteRequest, reply *VoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesRequest RPC request.
type AppendEntriesRequest struct {
	LeaderID          int
	Term              int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Log
	LeaderCommitIndex int
}

// AppendEntriesResponse RPC request.
type AppendEntriesResponse struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	DPrintf(3, "Peer [%d] received AppendEntries request: %+v\n", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Ignore if received from a old leader
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Reset election timer
	rf.heartbeat <- true

	if args.Term > rf.currentTerm || rf.votedFor != args.LeaderID {
		// Follow new leader
		DPrintf(1, "Peer [%d] starts following new leader [%d] at term [%d]\n", rf.me, args.LeaderID, args.Term)
		rf.state = Follower
		rf.votedFor = args.LeaderID
		rf.currentTerm = args.Term
	}

	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) > 0 {
		rf.lastLogIndex = args.PrevLogIndex + 1
		rf.log[rf.lastLogIndex] = args.Entries[0]
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = Max(args.LeaderCommitIndex, rf.lastLogIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	rf.lastLogIndex++
	newLog := Log{rf.currentTerm, command}
	rf.log[rf.lastLogIndex] = newLog

	index := rf.lastLogIndex
	term := rf.currentTerm
	isLeader := true

	// Your code here (2B).
	req := &AppendEntriesRequest{rf.me, rf.currentTerm, index - 1, rf.log[index-1].Term, []Log{rf.log[index]}, rf.commitIndex}
	go rf.startLogReplication(req, index)

	return index, term, isLeader
}

func (rf *Raft) startLogReplication(req *AppendEntriesRequest, logIndex int) {
	nReplicated := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		resp := &AppendEntriesResponse{}
		if rf.sendAppendEntries(server, req, resp) {
			if resp.Success {
				nReplicated++
				// TODO: manipulate nextIndex[] and matchIndex[]
			} else {
				// TODO: solve log inconsistency
			}
		}
	}

	if nReplicated > len(rf.peers)/2 {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.commitIndex = logIndex
	}
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.init()
	return rf
}
