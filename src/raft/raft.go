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

	//DPrintf("In RequestVote: %d%d", rf.me, rf.CurrentTerm)
	if args.Term < rf.CurrentTerm {
		reply.CTerm = rf.CurrentTerm
		reply.VoteGranted = false
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

	DPrintf("%d(Term: %d), RecvRequestVote from %d(Term: %d)", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
	if rf.VoteFor == -1 || //REM: initialize to -1
		rf.VoteFor == args.CandidateId { //&&
		//rf.LastApplied <= args.LastLogIndex {
		//if rf.State == 0 || (rf.State == -1 && rf.VoteFor == args.CandidateId) {
		//reply.CTerm = rf.CurrentTerm
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId //!!
		rf.RequestVoteChan <- true    //should be only here
		return                        //should have return
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("SendRequestVote: from %d(Term: %d) to %d", args.CandidateId, args.Term, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return ok
	}
	if rf.State != 0 || args.Term != rf.CurrentTerm {
		return ok
	}
	if reply.CTerm > rf.CurrentTerm {
		rf.CurrentTerm = reply.CTerm
		rf.State = -1
		rf.VoteFor = -1
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
	CTerm   int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("%d(Term: %d) RecvAppendEntries from %d(Term: %d)", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
	if args.Term < rf.CurrentTerm {
		reply.CTerm = rf.CurrentTerm
		reply.Success = false
		return
	}

	//REM: Receiver rule 2, 3, 4, 5

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.State = -1
		rf.VoteFor = -1
	} //all puts before reply.CTerm = rf.CurrentTerm

	reply.CTerm = rf.CurrentTerm
	reply.Success = true
	rf.AppendEntriesChan <- true
	//rf.VoteFor = args.LeaderId //?

	//DPrintf("AppendEntries: %d%d, %d%d", args.LeaderId, args.Term, rf.me, rf.CurrentTerm)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf("SendAppendEntries from %d(Term: %d) to %d", args.LeaderId, args.Term, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	}
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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Slog = nil //REM:
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.RequestVoteChan = make(chan bool, 30)
	rf.AppendEntriesChan = make(chan bool, 30)
	rf.ElectWin = make(chan bool, 30)

	rf.State = -1

	seed1 := rand.NewSource(time.Now().UnixNano())
	rseed1 := rand.New(seed1)
	RaftElectionTimeout := time.Duration(200+rseed1.Intn(200)) * time.Millisecond
	//RaftElectionTimeout = (300 + time.Duration(rf.me)*50) * time.Millisecond
	HeartBeatTimeout := 100 * time.Microsecond
	DPrintf("Waiting:%d%v", rf.me, RaftElectionTimeout)
	// Your initialization code here (2A, 2B, 2C).
	//time.Sleep(time.Duration(rseed1.Intn(20)) * time.Millisecond)
	//time.Sleep(RaftElectionTimeout)
	go func() {
		//timeout := time.After(RaftElectionTimeout)
		for {
			//DPrintf("%d%d", me, rf.CurrentTerm)
			if rf.State == -1 { //follwer
				select {
				case <-time.After(RaftElectionTimeout):
					rf.State = 0
				case <-rf.AppendEntriesChan:
					continue
				case <-rf.RequestVoteChan:
					continue
					//no default
				}
			} else if rf.State == 0 {
				rf.CurrentTerm += 1
				rf.VoteFor = rf.me
				//REM: reset the election timer
				go func() {
					succeesNum := 1
					//DPrintf("Leader- %d%d%d", rf.me, rf.CurrentTerm, succeesNum)
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me && rf.State == 0 { //rf.State may be
							//modified by other case
							args := &RequestVoteArgs{Term: rf.CurrentTerm,
								CandidateId:  rf.me,
								LastLogIndex: rf.LastApplied,
								LastLogTerm:  0, //rf.Slog[len(rf.Slog)-1].Term,
							} //REM: Lastxxx
							reply := &RequestVoteReply{}
							ok := rf.sendRequestVote(i, args, reply)
							if ok && reply.VoteGranted == true {
								succeesNum++
							}

						}
					}
					//DPrintf("Leader%d%d%d", rf.me, rf.CurrentTerm, succeesNum)
					if rf.State == 0 {
						if succeesNum > len(rf.peers)/2 {
							rf.State = 1
							DPrintf("LeaderWin: %d, and its Term: %d", rf.me, rf.CurrentTerm)
							rf.ElectWin <- true
						}
					}
				}()

				select {
				case <-time.After(RaftElectionTimeout): //timeout:
					continue
				case <-rf.AppendEntriesChan:
					rf.State = -1
				case <-rf.ElectWin:
					rf.State = 1 //already done at the other routine
				}

			} else if rf.State == 1 {
				quorum := 1
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.State == 1 { //rf.State may be
						//modified by other case
						args := &AppendEntriesArgs{Term: rf.CurrentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.LastApplied,
							PrevLogTerm:  0,   //rf.Slog[len(rf.Slog)-1].Term,
							Entries:      nil, //heartbeat empty
							LeaderCommit: rf.CommitIndex,
						} //REM: Prevxxx
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(i, args, reply)
						if ok {
							quorum++
						}
					}
				}

				if quorum <= len(rf.peers)/2 {
					rf.State = 0
				}
				//no default:
				//but one more case: in RPC request or response
				//if T > cT, will set to follower -1 directly
				//but here  && rf.State == 1  can avoid error
				time.Sleep(HeartBeatTimeout)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
