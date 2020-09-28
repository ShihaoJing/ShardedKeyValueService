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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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
	CurrentTerm int
	VotedFor    int
	OpLog       []Log

	// Volatile state on all servers
	state        int
	lastLogIndex int
	commitIndex  int
	lastApplied  int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int

	heartbeat chan bool
	applyCh   chan ApplyMsg
}

func (rf *Raft) appendLogEntry(entry Log, appendIndex int) {
	if appendIndex >= len(rf.OpLog) {
		rf.OpLog = append(rf.OpLog, entry)
	} else {
		rf.OpLog[appendIndex] = entry
	}
}

func randomElectionTimeout() int {
	rand.Seed(time.Now().UnixNano())
	min := 150
	max := 300
	return rand.Intn(max-min) + min
}

func (rf *Raft) init() {
	go rf.electionTiming()
	go rf.updateCommitIndex()
}

func (rf *Raft) updateCommitIndex() {
	ticker := time.NewTicker(50 * time.Millisecond)
	for range ticker.C {
		if rf.killed() {
			return
		}
		rf.mu.Lock()

		if rf.state == Leader {
			newCommitIndex := rf.commitIndex
			for i := rf.commitIndex + 1; i <= rf.lastLogIndex; i++ {
				nReplicated := 1
				for server := range rf.peers {
					if server == rf.me {
						continue
					}
					if rf.matchIndex[server] >= i {
						nReplicated++
					}
				}
				if nReplicated > len(rf.peers)/2 && rf.OpLog[i].Term == rf.CurrentTerm {
					DPrintf(1, "Server [%d] incrementing commitIndex to [%d]\n", rf.me, i)
					newCommitIndex = i
				}
			}
			rf.commitIndex = newCommitIndex
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			DPrintf(1, "Server [%d] commiting log at index[%d]: %+v", rf.me, rf.lastApplied, rf.OpLog[rf.lastApplied])
			rf.applyCh <- ApplyMsg{true, rf.OpLog[rf.lastApplied].Command, rf.lastApplied}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTiming() {
	timeoutRuration := randomElectionTimeout()
	electionTimer := time.NewTicker(time.Duration(timeoutRuration) * time.Millisecond)
	DPrintf(1, "Server [%d] timeout duration: [%d] ms\n", rf.me, timeoutRuration)
	for {
		if rf.killed() {
			DPrintf(1, "Peer [%d] got killed !!!\n", rf.me)
			return
		}
		select {
		case <-rf.heartbeat:
			DPrintf(3, "Peer [%d] received heatbeat. Reset Timer.\n", rf.me)
			electionTimer = time.NewTicker(time.Duration(timeoutRuration) * time.Millisecond)
		case <-electionTimer.C:
			rf.mu.Lock()
			DPrintf(5, "electionTiming: Holding lock")
			if rf.state != Leader {
				DPrintf(1, "Peer [%d] timout. Converts to candidate at new term [%d].\n", rf.me, rf.CurrentTerm+1)
				rf.state = Candidate
				rf.CurrentTerm++
				rf.VotedFor = rf.me
				rf.persist()
				go rf.election(rf.CurrentTerm, rf.lastLogIndex, rf.OpLog[rf.lastLogIndex].Term)
			}
			DPrintf(5, "electionTiming: Releasing lock")
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) election(electionTerm int, lastLogIndex int, lastLogTerm int) {
	req := VoteRequest{rf.CurrentTerm, rf.me, rf.lastLogIndex, rf.OpLog[rf.lastLogIndex].Term}
	voteReceived := make(chan interface{})
	var wg sync.WaitGroup
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		wg.Add(1)
		go rf.requestVote(server, req, voteReceived, &wg)
	}

	nVotes := 1
	timer := time.NewTimer(20 * time.Millisecond)
	for wait := true; wait; {
		select {
		case <-voteReceived:
			nVotes++
		case <-timer.C:
			wait = false
		}
	}

	DPrintf(1, "Candidate [%d] received total [%d] votes before timeout.", rf.me, nVotes)

	rf.mu.Lock()
	DPrintf(5, "startElection: Holding lock")
	if nVotes > len(rf.peers)/2 && rf.state == Candidate && rf.CurrentTerm == electionTerm {
		DPrintf(1, "***** Candidate [%d] converts to leader *****\n", rf.me)
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for server := range rf.peers {
			rf.nextIndex[server] = rf.lastLogIndex + 1
			rf.matchIndex[server] = 0
		}
		// send one heartbeat message immediatly
		req := &AppendEntriesRequest{time.Now().UnixNano() / 1000, rf.me, rf.CurrentTerm, rf.lastLogIndex, rf.OpLog[rf.lastLogIndex].Term, []Log{}, rf.commitIndex}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			resp := &AppendEntriesResponse{}
			go rf.sendAppendEntries(i, req, resp)
		}
		go rf.startLogReplication()
	}
	DPrintf(5, "startElection: Releasing lock")
	rf.mu.Unlock()

	wg.Wait()
}

func (rf *Raft) requestVote(server int, req VoteRequest, voteReceived chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	resp := VoteResponse{}
	DPrintf(1, "Candidate [%d] sending vote request to peer [%d]\n", rf.me, server)
	if rf.sendRequestVote(server, &req, &resp) == false {
		return
	}

	DPrintf(1, "Candidate [%d] received vote response from peer [%d]: %+v\n", rf.me, server, resp)
	DPrintf(5, "requestVote: Holding lock")
	if resp.VoteGranted {
		voteReceived <- "Vote received"
	}

	rf.mu.Lock()
	if rf.CurrentTerm < resp.Term {
		rf.state = Follower
		rf.CurrentTerm = resp.Term
		rf.VotedFor = NilCandidateID
		rf.persist()
	}
	DPrintf(5, "requestVote: Releasing lock")
	rf.mu.Unlock()
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	DPrintf(2, "Server[%d] saving persist state: %d, %d, %+v\n", rf.me, rf.CurrentTerm, rf.VotedFor, rf.OpLog)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.OpLog)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var VotedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf(2, "Decoding persist state failure.")
	} else {
		rf.mu.Lock()
		DPrintf(2, "Server[%d] reading persist state: %d, %d, %+v\n", rf.me, currentTerm, VotedFor, log)
		rf.CurrentTerm = currentTerm
		rf.VotedFor = VotedFor
		rf.OpLog = log
		rf.mu.Unlock()
	}
}

// VoteRequest RPC request.
type VoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// VoteResponse RPC response.
type VoteResponse struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *VoteRequest, reply *VoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.state = Follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = NilCandidateID
		rf.persist()
	}

	// Reject vote if
	// 1. Has voted for someone else
	// 2. More up-to-date than candidate
	if rf.VotedFor != NilCandidateID || rf.OpLog[rf.lastLogIndex].Term > args.LastLogTerm || (rf.OpLog[rf.lastLogIndex].Term == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// Grant vote and reset election timer.
	rf.heartbeat <- true
	rf.state = Follower
	rf.VotedFor = args.CandidateID
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = true
	rf.persist()
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
	EventID           int64
	LeaderID          int
	Term              int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []Log
	LeaderCommitIndex int
}

// AppendEntriesResponse RPC request.
type AppendEntriesResponse struct {
	EventID int64
	Term    int
	Success bool
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesResponse) {
	DPrintf(3, "Server [%d] received AppendEntries request: %+v\n", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.EventID = args.EventID

	if args.Term < rf.CurrentTerm {
		// Ignore if received from a old leader
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	// Reset election timer
	rf.heartbeat <- true

	if args.Term > rf.CurrentTerm {
		// Follow new leader
		DPrintf(1, "Server [%d]'s term[%d] outdated by Server[%d]'s term[%d]. Convert to follower. \n", rf.me, rf.CurrentTerm, args.LeaderID, args.Term)
		rf.state = Follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = NilCandidateID
		rf.persist()
	}

	if rf.lastLogIndex < args.PrevLogIndex || rf.OpLog[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}

	appendIndex := args.PrevLogIndex
	for _, entry := range args.Entries {
		appendIndex++
		if appendIndex <= rf.lastLogIndex && rf.OpLog[appendIndex] != entry {
			// delete conflit entry and all that follow it
			rf.lastLogIndex = appendIndex - 1
		}
		rf.appendLogEntry(entry, appendIndex)
		rf.persist()
	}
	rf.lastLogIndex = Max(appendIndex, rf.lastLogIndex)

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommitIndex, rf.lastLogIndex)
		DPrintf(1, "Peer [%d] found new commit index [%d]", rf.me, rf.commitIndex)
	}

	reply.Term = rf.CurrentTerm
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
	newLog := Log{rf.CurrentTerm, command}
	rf.appendLogEntry(newLog, rf.lastLogIndex)
	rf.persist()

	index := rf.lastLogIndex
	term := rf.CurrentTerm
	isLeader := true
	return index, term, isLeader
}

func (rf *Raft) startLogReplication() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendLogEntries(server, rf.CurrentTerm)
	}
}

func (rf *Raft) sendLogEntries(server int, term int) {
	ticker := time.NewTicker(time.Duration(50+server*5) * time.Millisecond)
	for range ticker.C {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Leader || term != rf.CurrentTerm {
			DPrintf(1, "Server [%d] no longer a leader or term changed. Stop log replication.", rf.me)
			rf.mu.Unlock()
			return
		}
		DPrintf(1, "Server [%d]'s nextIndex: %+v\n", rf.me, rf.nextIndex)
		logIndex := rf.nextIndex[server]
		prevIndex := logIndex - 1
		entries := []Log{}
		if logIndex <= rf.lastLogIndex {
			entries = append(entries, rf.OpLog[logIndex])
		}
		req := &AppendEntriesRequest{time.Now().UnixNano() / 1000, rf.me, rf.CurrentTerm, prevIndex, rf.OpLog[prevIndex].Term, entries, rf.commitIndex}
		resp := &AppendEntriesResponse{}
		if logIndex <= rf.lastLogIndex {
			DPrintf(1, "Leader [%d] sending [%d]th entry to peer [%d]: %+v\n", rf.me, logIndex, server, req)
		} else {
			DPrintf(1, "Leader [%d] sending heartbeat entry to peer [%d]: %+v\n", rf.me, server, req)
		}
		rf.mu.Unlock()

		if rf.sendAppendEntries(server, req, resp) == false {
			continue
		}

		DPrintf(1, "Leader [%d] received response from server [%d]: %+v\n", rf.me, server, resp)
		rf.mu.Lock()
		if req.Term != rf.CurrentTerm {
			rf.mu.Unlock()
			DPrintf(1, "Server [%d] term [%d] changed to term [%d] during RPC. Stop log replication.", rf.me, req.Term, rf.CurrentTerm)
			return
		}
		if resp.Success {
			DPrintf(1, "Leader [%d] replicating [%d]th entry to peer [%d] succeed.\n", rf.me, logIndex, server)
			rf.matchIndex[server] = prevIndex
			if rf.nextIndex[server] <= rf.lastLogIndex {
				rf.nextIndex[server]++
			}
		} else {
			if resp.Term > rf.CurrentTerm {
				DPrintf(1, "Server [%d]'s term[%d] outdated by Server[%d]'s term[%d]. Convert to follower. \n", rf.me, rf.CurrentTerm, rf.me, resp.Term)
				rf.state = Follower
				rf.CurrentTerm = resp.Term
				rf.VotedFor = NilCandidateID
				rf.persist()
			} else {
				//TODO: optimize quick backup
				if rf.nextIndex[server] > 1 {
					rf.nextIndex[server]--
				}
			}
		}
		rf.mu.Unlock()
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

	rf.state = Follower
	rf.CurrentTerm = 0
	rf.VotedFor = NilCandidateID
	rf.OpLog = []Log{Log{0, "sentinel"}}
	rf.readPersist(persister.ReadRaftState())
	rf.lastLogIndex = len(rf.OpLog) - 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeat = make(chan bool)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.init()

	return rf
}
