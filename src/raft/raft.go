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
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
}

type SLogEntry struct {
	//Index 	int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh (chan ApplyMsg)
	command (interface{})

	CurrentTerm int
	VoteFor     int
	Slog        []SLogEntry

	CommitIndex int
	LastApplied int

	State int

	NextIndex  []int
	MatchIndex []int

	RequestVoteChan   (chan bool)
	AppendEntriesChan (chan bool)
	ElectWin          (chan bool)
	succeesNum        int

	OptimizeMultipleEntries int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = (rf.State == 1)
	return term, isleader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Slog)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintfC("ha? empty??????????????????????????????????")
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	//check if ok == nil
	d.Decode(&rf.VoteFor)
	d.Decode(&rf.Slog)
	DPrintfC("Starting server from persist: %d, Term %d, votefor %d, last log idx %d, last term %d-----------", rf.me, rf.CurrentTerm, rf.VoteFor, rf.GetLastLogIndex(), rf.GetLastLogTerm())

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	CTerm       int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("In RequestVote: %d%d%d", rf.me, rf.State, rf.CurrentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("In RequestVote: %d%d", rf.me, rf.CurrentTerm)
	if args.Term < rf.CurrentTerm {
		reply.CTerm = rf.CurrentTerm
		reply.VoteGranted = false
		rf.persist()
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = -1
		rf.VoteFor = -1
		//reply.VoteGranted = true // ??
	}

	reply.CTerm = rf.CurrentTerm
	reply.VoteGranted = false

	DPrintfC("%d(Term: %d, last log index %d, log term %d), RecvRequestVote from %d(Term: %d, last log index: %d, term: %d)", rf.me, rf.GetLastLogIndex(), rf.GetLastLogTerm(), rf.CurrentTerm, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	if (rf.VoteFor == -1 || //REM: initialize to -1
		rf.VoteFor == args.CandidateId) &&
		rf.UpToDate(args.LastLogIndex, args.LastLogTerm) {

		//if rf.State == 0 || (rf.State == -1 && rf.VoteFor == args.CandidateId) {
		//reply.CTerm = rf.CurrentTerm

		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId //!!
		rf.RequestVoteChan <- true    //should be only here

		rf.persist()
		return //should have return
	}

	rf.persist()
	return
}

func (rf *Raft) UpToDate(argIndex int, argTerm int) bool {
	if len(rf.Slog) == 0 {
		return true
	}
	rfLogTerm := rf.GetLastLogTerm()

	if argTerm > rfLogTerm { //rf.CurrentTerm
		return true
	} else if argTerm == rfLogTerm {
		return argIndex >= rf.GetLastLogIndex()
	} else {
		return false
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
	DPrintfC("SendRequestVote: from %d(Term: %d) to %d", args.CandidateId, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}
	if rf.State != 0 || args.Term != rf.CurrentTerm { //mistake: rf.State = 1
		return ok
	}
	if reply.CTerm > rf.CurrentTerm {
		rf.CurrentTerm = reply.CTerm
		rf.State = -1
		rf.VoteFor = -1
		return ok
	}

	//DPrintf("Leader%d%d%d", rf.me, rf.CurrentTerm, succeesNum)
	if reply.VoteGranted && rf.State == 0 {
		rf.succeesNum++
		if rf.succeesNum > len(rf.peers)/2 {
			rf.State = 1
			DPrintfC("LeaderWin: %d, and its Term: %d", rf.me, rf.CurrentTerm)
			rf.ElectWin <- true
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []SLogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	CTerm        int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintfB("%d(Term: %d, loglen: %d) RecvAppendEntries from %d(Term: %d, preIdx %d),", rf.me, rf.CurrentTerm, len(rf.Slog), args.LeaderId, args.Term, args.PrevLogIndex)
	if args.Term < rf.CurrentTerm {
		reply.CTerm = rf.CurrentTerm
		//reply.Success = false //not needed for passing the test
		//if rf.OptimizeMultipleEntries == 1 {
		//	reply.NextTryIndex = rf.GetLastLogIndex() + 1
		//}
		rf.persist()
		return
	}

	//REM: Receiver rule 2, 3, 4, 5

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = -1
		rf.VoteFor = -1
		//return //cannot do this
		//if rf.OptimizeMultipleEntries == 1 {
		//	reply.NextTryIndex = rf.GetLastLogIndex() + 1
		//}
	} //all puts before reply.CTerm = rf.CurrentTerm

	reply.CTerm = rf.CurrentTerm
	rf.AppendEntriesChan <- true
	//Reply false if log doesn’t contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	if args.PrevLogIndex > len(rf.Slog)-1 {
		reply.CTerm = rf.CurrentTerm
		reply.Success = false

		if rf.OptimizeMultipleEntries == 1 {
			reply.NextTryIndex = rf.GetLastLogIndex() + 1
		}
		rf.persist()
		return // without return, report error
	}
	if args.PrevLogIndex > 0 && // > 0??
		rf.Slog[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.CTerm = rf.CurrentTerm
		reply.Success = false

		if rf.OptimizeMultipleEntries == 1 {
			term := rf.Slog[args.PrevLogIndex].Term

			for reply.NextTryIndex = args.PrevLogIndex - 1; reply.NextTryIndex > 0 && rf.Slog[reply.NextTryIndex].Term == term; reply.NextTryIndex-- {
			}

			reply.NextTryIndex++
			//reply.NextTryIndex = rf.GetLastLogIndex() + 1. wrong, else Test (2B): rejoin of partitioned leader
		}

		rf.persist()
		return
	}

	//fmt.Println(len(rf.Slog), args.PrevLogIndex+1)
	//If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	if len(args.Entries) != 0 &&
		len(rf.Slog)-1 >= args.PrevLogIndex && //entry conflict, first must have the entry. len(rf.Slog) - 1 >= , not len(rf.Slog), not  len(rf.Slog)-1 >
		//actually this must satisfy, so we can remove this , will not affect working
		rf.isConflict(args.Entries, rf.Slog[args.PrevLogIndex+1:]) {
		rf.Slog = rf.Slog[:args.PrevLogIndex+1] // not match with args.Entries[0]

		DPrintfB("%d: %vgagaga", rf.me, rf.Slog)
		rf.Slog = append(rf.Slog, args.Entries...) //...
		DPrintfB("%v", rf.Slog)

	}

	if rf.OptimizeMultipleEntries == 1 {
		//reply.NextTryIndex = args.PrevLogIndex //
		reply.NextTryIndex = rf.GetLastLogIndex() + 1
		//fmt.Println(reply.NextTryIndex, args.PrevLogIndex)

	}

	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = Min(args.LeaderCommit, rf.GetLastLogIndex())
		//rf.applyCh <- ApplyMsg{Command: rf.Slog[rf.CommitIndex].Command, CommandIndex: rf.CommitIndex, CommandValid: true}
		//if rf.LastApplied < rf.CommitIndex {
		//	rf.LastApplied = rf.CommitIndex
		//}
		go rf.commitlog()
		//REM: incr lastApplied, and apply log to state machine
	}

	reply.Success = true

	//rf.VoteFor = args.LeaderId //?

	//DPrintf("AppendEntries: %d%d, %d%d", args.LeaderId, args.Term, rf.me, rf.CurrentTerm)

	rf.persist()
	return
}

func (rf *Raft) isConflict(args []SLogEntry, rfSlog []SLogEntry) bool {
	for i := 0; i < len(args); i++ {
		if i >= len(rfSlog) {
			return true //args is longer
		}
		if args[i] != rfSlog[i] {
			return true
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("SendAppendEntries from %d(Term: %d) to %d", args.LeaderId, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return ok
	}
	if rf.State != 1 || args.Term != rf.CurrentTerm {
		return ok
	}

	if reply.CTerm > rf.CurrentTerm {
		rf.CurrentTerm = reply.CTerm
		rf.State = -1
		rf.VoteFor = -1
		return ok
	}

	//DPrintfB("send entries: %d", server)

	if reply.Success == false { //handle log inconsistency.
		if rf.OptimizeMultipleEntries == 1 {
			rf.NextIndex[server] = reply.NextTryIndex
		} else {
			if rf.NextIndex[server] > rf.MatchIndex[server]+1 {
				rf.NextIndex[server]--
			}
		}
		if rf.OptimizeMultipleEntries != 1 {
			args := &AppendEntriesArgs{Term: rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.GetPrevLogIndex(server),
				PrevLogTerm:  rf.GetPrevLogTerm(server),      //rf.Slog[len(rf.Slog)-1].Term,
				Entries:      rf.Slog[rf.NextIndex[server]:], //heartbeat empty
				LeaderCommit: rf.CommitIndex,
			}
			//DPrintfB("resend entries: %d", server)
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
		}
		return ok
	}

	if reply.Success == true && args.Entries != nil { //not simple else

		rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries) // args.PrevLogIndex !!!! very important. should not be len(rf.Slog) + len(Entries)
		rf.NextIndex[server] = rf.MatchIndex[server] + 1              // not rf.NextIndex[server]++

		for N := len(rf.Slog) - 1; N > rf.CommitIndex; N-- {
			if rf.Slog[N].Term == rf.CurrentTerm {
				majorityN := 1
				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}
					if rf.MatchIndex[j] >= N {
						majorityN++
					}
				}
				if majorityN > len(rf.peers)/2 {
					rf.CommitIndex = N
					DPrintfC("Successfully commit index %d by %d", N, rf.me)
					DPrintUpper("Successfully commit index %d by %d", N, rf.me)
					rf.persist()
					//go rf.commitlog()
					go func() {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
							DPrintUpper("cnt %d", i)
							rf.applyCh <- ApplyMsg{Command: rf.Slog[i].Command, CommandIndex: i, CommandValid: true}
							DPrintUpper("cnt2 %d", i)
						}

						rf.LastApplied = rf.CommitIndex
						DPrintUpper("Rep Successfully commit index %d by %d", N, rf.me)
					}()
					break
				}
			}
			//if rf.Slog[N].Term < rf.CurrentTerm {
			//	break
			//} //wrong for Test (2B): leader backs up quickly over incorrect follower logs
		}

	}

	return ok
}

func (rf *Raft) commitlog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
		rf.applyCh <- ApplyMsg{Command: rf.Slog[i].Command, CommandIndex: i, CommandValid: true}
	}
	rf.LastApplied = rf.CommitIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := 0 //-1
	term := 0  //-1
	isLeader := true

	// Your code here (2B).
	if rf.State != 1 {
		//return 0, rf.CurrentTerm, false
		return 0, 0, false
	}

	//rf.mu.Lock()
	rf.command = command
	//commandInt, _ := command.(int)
	rf.Slog = append(rf.Slog, SLogEntry{Term: rf.CurrentTerm, Command: command})
	//REM: respond after entry applied to state machine
	index = len(rf.Slog) - 1 //rf.GetLastLogIndex() + 1 //
	//index = rf.CommitIndex + 1
	//rf.mu.Unlock()

	DPrintfB("Start %d, next index %d", rf.me, index)

	term = rf.CurrentTerm
	isLeader = (rf.State == 1)
	return index, term, isLeader
}

//
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.Slog) - 1
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.Slog[len(rf.Slog)-1].Term //len(rf.Slog) >= 1

}

func (rf *Raft) GetPrevLogIndex(i int) int {
	return rf.NextIndex[i] - 1
}

func (rf *Raft) GetPrevLogTerm(i int) int {
	return rf.Slog[rf.NextIndex[i]-1].Term
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
	rf.applyCh = applyCh

	rf.CurrentTerm = 0 // not 0
	rf.VoteFor = -1
	rf.Slog = []SLogEntry{}                                     //nil //REM:
	rf.Slog = append(rf.Slog, SLogEntry{Term: 0, Command: nil}) // Term start from 1, so is Slog index
	rf.CommitIndex = 0
	rf.LastApplied = 0
	//rf.NextIndex = []int{}
	//rf.MatchIndex = [len(rf.peers)]int{}
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex = append(rf.NextIndex, 1+rf.GetLastLogIndex())
		rf.MatchIndex = append(rf.MatchIndex, 0) //REM:
	}

	rf.RequestVoteChan = make(chan bool)
	rf.AppendEntriesChan = make(chan bool)
	rf.ElectWin = make(chan bool)

	rf.OptimizeMultipleEntries = 1

	rf.State = -1

	seed1 := rand.NewSource(time.Now().UnixNano())
	rseed1 := rand.New(seed1)
	RaftElectionTimeout := time.Duration(300+rseed1.Intn(200)) * time.Millisecond
	//RaftElectionTimeout = (300 + time.Duration(rf.me)*50) * time.Millisecond
	HeartBeatTimeout := 120 * time.Millisecond //time.Microsecond
	DPrintf("Waiting:%d%v", rf.me, RaftElectionTimeout)
	// Your initialization code here (2A, 2B, 2C).
	//time.Sleep(time.Duration(rseed1.Intn(20)) * time.Millisecond)
	//time.Sleep(RaftElectionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintfC("Starting server from persist: %d, Term %d, votefor %d, last log idx %d, last term %d-----------", rf.me, rf.CurrentTerm, rf.VoteFor, rf.GetLastLogIndex(), rf.GetLastLogTerm())

	go func() {
		//timeout := time.After(RaftElectionTimeout)
		for {
			//DPrintf("%d%d", me, rf.CurrentTerm)
			switch rf.State {
			case -1:
				{ //follwer
					select {
					case <-time.After(RaftElectionTimeout):
						rf.State = 0
					case <-rf.AppendEntriesChan:
						continue
					case <-rf.RequestVoteChan:
						continue
						//no default
					}
				}
			case 0:
				{
					rf.mu.Lock()

					rf.CurrentTerm += 1
					rf.VoteFor = rf.me
					rf.succeesNum = 1

					args := &RequestVoteArgs{Term: rf.CurrentTerm,
						CandidateId:  rf.me,
						LastLogIndex: rf.GetLastLogIndex(),
						LastLogTerm:  rf.GetLastLogTerm(), //rf.Slog[len(rf.Slog)-1].Term,
					} //REM: Lastxxx
					//reply := &RequestVoteReply{} //should pass value instead of reference!!!

					rf.mu.Unlock()

					//REM: reset the election timer
					//go func() {
					//DPrintf("Leader- %d%d%d", rf.me, rf.CurrentTerm, succeesNum)
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me && rf.State == 0 { //rf.State may be
							//modified by other case
							go rf.sendRequestVote(i, args, &RequestVoteReply{}) //reply should pass value instead of reference!!!

						}
					}

					//}()

					select {
					case <-time.After(RaftElectionTimeout): //timeout:
						continue
					case <-rf.AppendEntriesChan:
						rf.State = -1
					case <-rf.ElectWin:
						rf.mu.Lock()
						rf.State = 1 //already done at the other routine
						rf.NextIndex = nil
						rf.MatchIndex = nil
						for i := 0; i < len(rf.peers); i++ {
							rf.NextIndex = append(rf.NextIndex, 1+rf.GetLastLogIndex())
							rf.MatchIndex = append(rf.MatchIndex, 0) //REM:
						}
						rf.mu.Unlock()
					}

				}
			case 1:
				{
					rf.mu.Lock()
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me && rf.State == 1 { //rf.State may be
							//modified by other case
							//rf.mu.Lock()
							DPrintfB("Me: %d (Term %d, loglen %d), send to %d, PrevLogIndex: %d", rf.me, rf.CurrentTerm, len(rf.Slog), i, rf.GetPrevLogIndex(i))
							args := &AppendEntriesArgs{Term: rf.CurrentTerm,
								LeaderId:     rf.me,
								PrevLogIndex: rf.GetPrevLogIndex(i),
								PrevLogTerm:  rf.Slog[rf.NextIndex[i]-1].Term, //rf.GetPrevLogTerm(i), //rf.Slog[len(rf.Slog)-1].Term,
								Entries:      nil,                             //heartbeat empty
								LeaderCommit: rf.CommitIndex,
							}

							//fmt.Println(rf.NextIndex[i], len(rf.Slog))
							if rf.NextIndex[i] <= len(rf.Slog)-1 {
								args.Entries = rf.Slog[rf.NextIndex[i]:]
							}
							//rf.mu.Unlock()
							go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
						}
					}
					rf.mu.Unlock()
					//no default:
					//but one more case: in RPC request or response
					//if T > cT, will set to follower -1 directly
					//but here  && rf.State == 1  can avoid error
					time.Sleep(HeartBeatTimeout)
				}
			}
		}
	}()

	return rf
}
