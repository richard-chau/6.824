package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf4(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Optype string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage map[string]string
	GetChan (chan bool)
	PAChan  (chan bool)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.rf.State != 1 {
		reply.WrongLeader = true
		//reply rf.VoteFor
		//reply.Err
		return
	}
	// You'll have to add definitions here.
	//if args.Snum exist
	//key := args.Key
	op := Op{Optype: "Get", Key: args.Key}
	DPrintf4("Get %v", op)
	kv.rf.Start(op) //index1, term1, ok

	<-kv.GetChan

	reply.Value = kv.storage[op.Key]
	reply.WrongLeader = false
	reply.Err = ""
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.rf.State != 1 {
		reply.WrongLeader = true
		//reply rf.VoteFor
		//reply.Err
		return
	}

	op := Op{Optype: args.Op, Key: args.Key, Value: args.Value}
	//if args.Snum exist
	kv.rf.Start(op)

	<-kv.PAChan

	reply.WrongLeader = false
	reply.Err = ""
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
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

	kv.GetChan = make(chan bool)
	kv.PAChan = make(chan bool)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.storage = make(map[string]string)

	go func() {
		DPrintf4("start listening")
		for m := range kv.applyCh {
			//do get / append / put
			DPrintf4("server %d received applych", kv.me)
			op := m.Command.(Op)
			switch op.Optype {
			case "Get": //do nothing
				kv.mu.Lock()
				if kv.rf.State == 1 {
					kv.GetChan <- true
				}
				kv.mu.Unlock()
			case "Put":
				kv.mu.Lock()
				if kv.rf.State == 1 {
					DPrintf4("will put %v %v", op.Key, op.Value)
					kv.storage[op.Key] = op.Value
					kv.PAChan <- true
				}
				kv.mu.Unlock()
			case "Append":
				kv.mu.Lock()
				if kv.rf.State == 1 {
					kv.storage[op.Key] = kv.storage[op.Key] + op.Value
					kv.PAChan <- true
				}
				kv.mu.Unlock()
			}
		}
	}()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
