package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug0 = 0
const Debug4 = 0
const Debug5 = 0
const MixServer = 0

func DPrintf4(format string, a ...interface{}) (n int, err error) {
	if Debug4 > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf5(format string, a ...interface{}) (n int, err error) {
	if Debug5 > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintf0(format string, a ...interface{}) (n int, err error) {
	if Debug0 > 0 {
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

	Cid  int64
	Snum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage    map[string]string
	commFilter map[int64][]int

	//GetChan (chan bool)
	//PAChan  (chan bool)
	notify map[int]chan Op
}

func (kv *KVServer) FilterSnum(cid int64, snum int) bool {
	DPrintf5("%v, Filter, Len: %d, value %v", cid, len(kv.commFilter[cid]), kv.commFilter[cid])
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	que := kv.commFilter[cid]
	for i := len(que) - 1; i >= 0; i = i - 1 {
		if que[i] == snum {
			DPrintf5("%d filter success cli %v with snum=%d", kv.me, cid, snum)
			return true
		}
		if que[i] < snum {
			DPrintf5("%d filter non-exist cli %v with snum=%d", kv.me, cid, snum)
			return false
		}
	}
	DPrintf5("%d filter non-exist cli %v with snum=%d", kv.me, cid, snum)
	return false
}

func (kv *KVServer) WHLog(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	WaitTimeout := time.Duration(1000) * time.Millisecond
	if isLeader {

		kv.mu.Lock()
		notifyChan, ok := kv.notify[index]
		if !ok {
			kv.notify[index] = make(chan Op)
			notifyChan, _ = kv.notify[index]
		}
		kv.mu.Unlock()

		select {
		case opChan := <-notifyChan:
			if opChan == op {
				return true
			} else {
				return false
			}
		case <-time.After(WaitTimeout): //network partition, is leader, but cannot commit
			return false
		}
	} else { //not leader, or maybe is leader and commit but just get off when recv applyChan
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf5("me %d", kv.me)
	if kv.rf.State != 1 {
		reply.WrongLeader = true
		//reply rf.VoteFor
		//reply.Err
		return
	}
	// You'll have to add definitions here.
	if kv.FilterSnum(args.Cid, args.Snum) {
		DPrintf5("Filter Get, Len: %d", kv.commFilter[args.Cid])
		kv.mu.Lock()
		reply.Value = kv.storage[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = ""
		return
	}

	DPrintf5("%d Get from %v, Key: %v, snum: %d", kv.me, args.Cid, args.Key, args.Snum)
	op := Op{Optype: "Get", Key: args.Key, Snum: args.Snum, Cid: args.Cid}
	success := kv.WHLog(op)

	if !success {
		reply.WrongLeader = true
	} else {
		//var exist bool
		kv.mu.Lock()
		//reply.Value = kv.storage[op.Key]
		val, exist := kv.storage[op.Key]
		kv.mu.Unlock()

		if exist {
			reply.Value = val
			reply.Err = ""
		} else {
			reply.Err = "No such Key"
		}
		reply.WrongLeader = false

	}

	//key := args.Key

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.rf.State != 1 {
		reply.WrongLeader = true
		//reply rf.VoteFor
		//reply.Err
		return
	}

	if kv.FilterSnum(args.Cid, args.Snum) {
		DPrintf5("Filter Put/Append, Len: %d", kv.commFilter[args.Cid])
		reply.WrongLeader = false
		reply.Err = ""
		return
	}

	op := Op{Optype: args.Op, Key: args.Key, Value: args.Value, Snum: args.Snum, Cid: args.Cid}
	DPrintf5("%d Appended from %d, Key: %v, snum: %d", kv.me, args.Cid, args.Key, args.Snum)
	success := kv.WHLog(op)

	if success {
		//var exist bool
		//reply.Value = kv.storage[op.Key]
		reply.WrongLeader = false
		reply.Err = ""
	} else {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) ApplyOp(op Op) {
	switch op.Optype {
	case "Put":
		kv.storage[op.Key] = op.Value
	case "Append":
		kv.storage[op.Key] = kv.storage[op.Key] + op.Value
	default:
		return
	}
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

	//kv.GetChan = make(chan bool)
	//kv.PAChan = make(chan bool)
	kv.notify = make(map[int]chan Op) //->chan array
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.storage = make(map[string]string)
	kv.commFilter = make(map[int64][]int)

	go func() {
		DPrintf5("start listening %d", kv.me)
		for m := range kv.applyCh {
			//do get / append / put
			DPrintf5("server %d received applych", kv.me)
			op := m.Command.(Op)

			kv.mu.Lock()
			//DPrintf5("%v will put %v %v with snum %d-----------------------------", op.Cid, op.Key, op.Value, op.Snum)
			if !kv.FilterSnum(op.Cid, op.Snum) {
				if kv.commFilter[op.Cid] == nil {
					kv.commFilter[op.Cid] = make([]int, 0)
				}
				kv.commFilter[op.Cid] = append(kv.commFilter[op.Cid], op.Snum)
				kv.ApplyOp(op)
			}

			notifyChan, ok := kv.notify[m.CommandIndex]
			if ok {
				notifyChan <- op
			}
			kv.mu.Unlock()
		}
	}()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
