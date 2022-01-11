package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"

	"bytes"

	"../labgob"
	"../labrpc"

	"math/rand"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
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
	CommandTerm  int
	Snapshot     []byte
}

// log entry fields
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

const (
	// server roles
	Follower = iota
	Candidate
	Leader

	// timer related constants (in ms)
	ElectionTimeoutMin           = 500
	ElectionTimeoutRange         = 500
	ElectionTimeoutPeriodicCheck = 100
	HeartbeatInterval            = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain

	// roles and timeouts associated with changing roles
	role                     int
	electionTimeout          time.Duration
	electionTimeoutResetTime time.Time
	heartbeatInterval        time.Duration
	leaderId                 int

	applyCond          *sync.Cond // condition variable to signal raft to apply log or snapshot to state machine
	InstallNewSnapshot bool

	// Persistent state on all servers
	log         []LogEntry
	currentTerm int
	votedFor    int

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// snapshot metadata, include as persistent state
	// need to add, as an offset, lastIncludedIndex and lastIncludedTerm from snapshot to various functions
	// that attempt to read from the log at a specific index, since the log may have been compacted
	lastIncludedIndex int
	lastIncludedTerm  int
}

//////////////////////////////////////////////////////////////////////
// raft functions that help with code organization and explicitness //
//////////////////////////////////////////////////////////////////////
func (rf *Raft) convertToFollower() {
	DPrintf("%d: convertToFollower()\n", rf.me)
	oldRole := rf.role

	rf.role = Follower
	rf.resetElectionTimeout()

	// if we were a leader, need to restart election timeout
	if oldRole == Leader {
		go rf.electionTimeoutPeriodicCheck()
	}
}

func (rf *Raft) convertToCandidate() {
	DPrintf("%d: convertToCandidate()\n", rf.me)
	rf.role = Candidate
	rf.resetElectionTimeout()
}

func (rf *Raft) convertToLeader() {
	DPrintf("%d: convertToLeader()\n", rf.me)
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	nextIndex := rf.getLastLogIndex() + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = nextIndex
	}

	// start heartbeat timer
	go rf.heartbeat()
}

func (rf *Raft) updateTerm(term int) {
	DPrintf("%d: updateTerm(%d)\n", rf.me, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persistState()
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Duration(rand.Intn(ElectionTimeoutRange)+ElectionTimeoutMin) * time.Millisecond
	rf.electionTimeoutResetTime = time.Now()
}

/* NOTE: index is 1-based and is based on the entry's index, NOT the slice index (they are the same if no snapshot) */
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// cannot simply get the log entry by index anymore, since the log may have been compacted
func (rf *Raft) getLogEntry(index int) LogEntry {
	logEntry := rf.log[index-rf.lastIncludedIndex]
	if logEntry.Index != index {
		DPrintf("%d: getLogEntry: log entry index does not match index, logIndex: %d, index: %d\n", rf.me, logEntry.Index, index)
		panic("getLogEntry: log entry index does not match index")
	}
	return logEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.role == Leader)

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistState() {
	// Your code here (2C).
	state := rf.encodeState()
	rf.persister.SaveRaftState(state)
	// DPrintf("%d: persistState() completed\n", rf.me)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersistedState(data []byte) {
	if data == nil || len(data) < 1 {
		rf.log = []LogEntry{{0, 0, nil}} // simplifies raft logic by having a dummy entry that serves as the first entry in the log
		rf.currentTerm = 0
		rf.votedFor = -1
		return
	}

	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		log         []LogEntry
		currentTerm int
		votedFor    int
	)
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		DPrintf("%d: readPersistedState() failed\n", rf.me)
		rf.log = []LogEntry{{0, 0, nil}}
		rf.currentTerm = 0
		rf.votedFor = -1
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
}

//
// restore previously persisted snapshot.
//

type dupeOp struct {
	SeqNum int
	Value  string
}

func (rf *Raft) readPersistedSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var (
		kvStore           map[string]string
		dupeTable         map[int64]dupeOp
		lastIncludedIndex int
		lastIncludedTerm  int
	)
	if d.Decode(&kvStore) != nil ||
		d.Decode(&dupeTable) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("%d: readPersistedSnapshot() failed\n", rf.me)
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
	} else {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}

	DPrintf("%d: readPersistedSnapshot() completed, index: %d, term: %d\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) GetRaftStateSize() float64 {
	return float64(rf.persister.RaftStateSize())
}

func (rf *Raft) PersistStateAndSnapshotWithLock(snapshot []byte, index int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persistStateAndSnapshot(snapshot, index, term)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte, index int, term int) {
	// return immediately if the argument snapshot is stale
	if index <= rf.lastIncludedIndex {
		return
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	state := rf.encodeState()

	rf.persister.SaveStateAndSnapshot(state, snapshot)

	rf.trimLog()

	DPrintf("%d: persistStateAndSnapshot() completed index: %d, term: %d, log: %+v\n", rf.me, index, term, rf.log)
}

func (rf *Raft) trimLog() {
	// DPrintf("%d: TrimLog() Before %+v\n", rf.me, rf.log)
	foundLastSnapshotEntry := false

	// discard log entries that are a part of the snapshot
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == rf.lastIncludedIndex &&
			rf.log[i].Term == rf.lastIncludedTerm {
			rf.log = rf.log[i+1:]
			// prepend log with dummy entry
			rf.log = append([]LogEntry{{rf.lastIncludedIndex, rf.lastIncludedTerm, nil}}, rf.log...)
			foundLastSnapshotEntry = true
			break
		} else if rf.log[i].Index > rf.lastIncludedIndex || rf.log[i].Term > rf.lastIncludedTerm {
			break
		}
	}

	if !foundLastSnapshotEntry {
		// no match, discard everything
		rf.log = []LogEntry{{rf.lastIncludedIndex, rf.lastIncludedTerm, nil}}
	}
	// DPrintf("%d: TrimLog() After %+v\n", rf.me, rf.log)
	rf.persistState()
}

////////////////////////////////////////
// RPC arguments/replies and handlers //
////////////////////////////////////////

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	/**
	 * in the case of failure, we can speed up AE rollback by providing ConflictTerm, ConflictIndex and LogLength
	 * on success, these are ignored (supply anyway for debugging)
	 * ConflictTerm: term in the conflicting entry at PrevLogIndex (-1 if there is no entry)
	 * ConflictIndex: index of the first entry for ConflictTerm (-1 if there is no entry)
	 * LogLength: follower's last log index
	 * 3 cases that need to be looked at:
	 * 1. follower does not have a log at PrevLogIndex
	 * -> nextIndex = LastLogIndex + 1 (follower's log is too short)
	 * 2. leader does not have ConflictTerm in log
	 * -> nextIndex = ConflictIndex (skip over the conflicting entries of that term)
	 * 3. leader does have ConflictTerm in log
	 * -> nextIndex = leader's last entry for ConflictTerm
	 **/
	ConflictTerm  int
	ConflictIndex int
	LastLogIndex  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Offset and Done not used in this implementation since we will be sending the entire snapshot, not chunks
	// Offset            int
	// Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d: RequestVote()\n", rf.me)
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.convertToFollower()
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		if rf.role == Follower {
			rf.resetElectionTimeout()
		}
		rf.votedFor = args.CandidateId
		rf.persistState()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d: AppendEntries: args:%+v\nlog:%+v\n", rf.me, args, rf.log)

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.convertToFollower()
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// at this point, we know that AppendEntries was called by the current leader
	if rf.role == Follower {
		rf.resetElectionTimeout()
	}
	rf.leaderId = args.LeaderId

	// make our own prevLogIndex and entryNum?

	// in the event we receive an AppendEntries from a leader that hasn't updated nextIndex yet from a previous AppendEntries,
	// use our raft property of log matching to shift prevLogIndex if PrevLogIndex < lastIncludedIndex
	// (IMPORTANT) as well as adjusting which entries we care about using i
	prevLogIndex := args.PrevLogIndex
	i := 0
	if args.PrevLogIndex < rf.lastIncludedIndex {
		prevLogIndex = rf.lastIncludedIndex
		i = rf.lastIncludedIndex - args.PrevLogIndex
	}

	if prevLogIndex > rf.getLastLogIndex() { // if the prevLogIndex is not in the log
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
	} else if rf.getLogEntry(prevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getLogEntry(prevLogIndex).Term
		reply.ConflictIndex = 0
		// find the first entry of the conflicting term
		for i := prevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
			if rf.getLogEntry(i).Term < reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
	} else {
		reply.Success = true

		// i := 0
		// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		for ; i < len(args.Entries); i++ {
			entryIndex := args.Entries[i].Index
			if entryIndex > rf.getLastLogIndex() || entryIndex < rf.lastIncludedIndex {
				break
			}

			if args.Entries[i].Term != rf.getLogEntry(entryIndex).Term {
				rf.log = rf.log[:entryIndex-rf.lastIncludedIndex]
				break
			}
		}

		// append any new entries not already in the log
		for ; i < len(args.Entries); i++ {
			rf.log = append(rf.log, args.Entries[i])
		}

		rf.persistState()

		if args.LeaderCommit > rf.commitIndex {
			// min(leaderCommit, lastLogIndex)
			if args.LeaderCommit < rf.getLastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastLogIndex()
			}

			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		}
	}

	reply.LastLogIndex = rf.getLastLogIndex()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d: InstallSnapshot\n", rf.me)
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
		rf.convertToFollower()
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// at this point, we know that InstallSnapshot was called by the current leader
	if rf.role == Follower {
		rf.resetElectionTimeout()
	}
	rf.leaderId = args.LeaderId

	rf.persistStateAndSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	} else if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.InstallNewSnapshot = true
	rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%d: Start: %+v\n", rf.me, command)

	if rf.role == Leader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		isLeader = true
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.persistState()

		// send AppendEntries RPCs to all other servers
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if rf.getLastLogIndex() >= rf.nextIndex[i] {
				go rf.sendAndHandleAppendEntries(i)
			}
		}
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
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

//
// the service or tester wants to create a Raft server. the ports
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

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("%d: Make()\n", rf.me)
	rf.applyCond = sync.NewCond(&rf.mu)

	// start with persistent states
	rf.role = Follower
	// usually, election timeout for raft is 150-300ms
	// but we have constraint of 10 heartbeats per second and 5 seconds to elect
	// so we tentatively set election timeout to 500-1000ms
	rand.Seed(time.Now().UnixNano())
	rf.resetElectionTimeout()
	rf.heartbeatInterval = time.Duration(HeartbeatInterval) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersistedState(persister.ReadRaftState())
	rf.readPersistedSnapshot(persister.ReadSnapshot())

	// commitIndex and lastApplied are at least on the lastIncludedIndex from snapshot
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	// handle potentially stale logs
	rf.trimLog()

	go rf.electionTimeoutPeriodicCheck()
	go rf.apply(applyCh)

	return rf
}

///////////////////////////////////////////////////////////////
// separate goroutines for important tasks to avoid blocking //
///////////////////////////////////////////////////////////////

// periodically check if election timeout has passed
// if election timeout has passed, then start election
func (rf *Raft) electionTimeoutPeriodicCheck() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()

		if rf.role == Leader {
			rf.mu.Unlock()
			return
		}

		timeSince := time.Now().Sub(rf.electionTimeoutResetTime)
		if timeSince > rf.electionTimeout {
			if rf.role == Follower {
				rf.convertToCandidate()
			}

			if rf.role == Candidate {
				go rf.startElection()
			}
		}
		rf.mu.Unlock()

		time.Sleep(ElectionTimeoutPeriodicCheck)
	}
}

func (rf *Raft) heartbeat() {
	for {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go rf.sendAndHandleAppendEntries(i)
		}

		rf.mu.Unlock()
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		// wait for commitIndex to change
		for !rf.InstallNewSnapshot && rf.commitIndex == rf.lastApplied {
			rf.applyCond.Wait()
		}
		DPrintf("%d: apply InstallNewSnapshot: %t, commitIndex: %d, lastApplied: %d, log: %+v\n", rf.me, rf.InstallNewSnapshot, rf.commitIndex, rf.lastApplied, rf.log)

		var applyMsg ApplyMsg

		// either we have a new snapshot (prioritize it) or we have a new committed log
		if rf.InstallNewSnapshot {
			rf.InstallNewSnapshot = false
			applyMsg = ApplyMsg{
				CommandValid: false,
				Snapshot:     rf.persister.ReadSnapshot(),
			}
		} else {
			rf.lastApplied++
			applyMsg = ApplyMsg{
				CommandValid: true,
				Command:      rf.getLogEntry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.getLogEntry(rf.lastApplied).Term,
			}
		}
		rf.mu.Unlock() // avoid potential deadlock by unlocking before applyCh send, where applyCh receive on kvserver is inadvertently blocked by rf.mu
		// example: kvserver obtains lock on kv.mu, then calls rf.start() which tries to acquire rf.mu
		// however, rf.mu is locked here due to applyCh blocking
		// applyCh continues to block because kvserver already has a lock on its own apply method, thus rf.mu never releases lock here
		applyCh <- applyMsg
	}
}

////////////////////////////////////
// sending and handling raft RPCs //
////////////////////////////////////
func (rf *Raft) startElection() {
	rf.mu.Lock()

	DPrintf("%d: startElection\n", rf.me)

	rf.updateTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.persistState()
	rf.resetElectionTimeout()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	numVotes := 1

	rf.mu.Unlock()

	var sendAndHandleRequestVote = func(server int) {
		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(server, &args, &reply)
		if !ok {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("%d: sendAndHandleRequestVote(%d) reply: %+v\n", rf.me, server, reply)

		if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
			rf.convertToFollower()
			return
		}
		// drop the (old) reply, since the raft has higher term than when it sent the request
		if rf.currentTerm != args.Term {
			return
		}

		// process reply
		if rf.role == Candidate && reply.VoteGranted {
			numVotes++ // closure captures the value of numVotes
			if numVotes > len(rf.peers)/2 {
				rf.convertToLeader()
			}
		}
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go sendAndHandleRequestVote(i)
	}
}

func (rf *Raft) sendAndHandleAppendEntries(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	// DPrintf("%d: sendAndHandleAppendEntries(%d) start, nextIndex: %+v\n", rf.me, server, rf.nextIndex)

	// if the follower has fallen too far behind, send them a screenshot
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.sendAndHandleInstallSnapshot(server)
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.getLogEntry(rf.nextIndex[server] - 1).Term,
		Entries:      rf.log[rf.nextIndex[server]-rf.lastIncludedIndex:],
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("%d: sendAndHandleAppendEntries(%d) reply: %+v\n      entries: %+v\n", rf.me, server, reply, args.Entries)

	// immediately step down if the leader is found to be out of date
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.convertToFollower()
		return
	}

	// drop old reply
	if rf.currentTerm != args.Term {
		return
	}

	// process reply if still leader
	if rf.role == Leader {
		if reply.Success {
			if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// If there exists an N such that N > commitIndex,
			// a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm: set commitIndex = N
			for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
				if rf.getLogEntry(i).Term != rf.currentTerm {
					continue
				}

				numMatch := 1
				for j := range rf.peers {
					if j == rf.me {
						continue
					}

					if rf.matchIndex[j] >= i {
						numMatch++
					}
				}

				if numMatch > len(rf.peers)/2 {
					rf.commitIndex = i
				}
			}

			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		} else {
			/* make use of ConflictTerm and ConflictIndex to update nextIndex more efficiently */

			if reply.ConflictTerm == -1 { // case 1: no log was found at PrevLogIndex
				rf.nextIndex[server] = reply.LastLogIndex + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex // case 2: leader does not have ConflictTerm in log

				for i := rf.getLastLogIndex(); i >= rf.lastIncludedIndex; i-- {
					if rf.getLogEntry(i).Term == reply.ConflictTerm {
						rf.nextIndex[server] = i + 1 // case 3: leader has ConflictTerm in log, set nextIndex equal to the entry index directly after ConflictTerm entries
						break
					} else if rf.getLogEntry(i).Term < reply.ConflictTerm { // case 2 preserved
						break
					}
				}
			}

			go rf.sendAndHandleAppendEntries(server)
		}
	}

	// DPrintf("%d: sendAndHandleAppendEntries end(%d), nextIndex: %+v\n", rf.me, server, rf.nextIndex)
}

func (rf *Raft) sendAndHandleInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	DPrintf("%d: sendAndHandleInstallSnapshot(%d) start, nextIndex: %+v\n", rf.me, server, rf.nextIndex)

	// rf.readPersistedSnapshot(rf.persister.ReadSnapshot())
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}

	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// immediately step down if the leader is found to be out of date
	if reply.Term > rf.currentTerm {
		rf.updateTerm(reply.Term)
		rf.convertToFollower()
		return
	}

	// drop old reply
	if rf.currentTerm != args.Term {
		return
	}

	// process reply if still leader
	if rf.role == Leader {
		// TODO: is this correct to do?
		// rf.nextIndex[server] = rf.getLastLogIndex() + 1
		// rf.nextIndex[server] = rf.lastIncludedIndex + 1
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		// rf.matchIndex[server] = rf.lastIncludedIndex <- not necessarily true...what if the snapshot failed?
	}

	DPrintf("%d: sendAndHandleInstallSnapshot(%d) end, nextIndex: %+v\n", rf.me, server, rf.nextIndex)

	// TODO: do I need to send here or just let heartbeat handle it?
	// go rf.sendAndHandleAppendEntries(server)
}
