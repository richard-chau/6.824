package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me         int64 //?
	lastLeader int
	snum       int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.snum = 0
	ck.lastLeader = 0
	ck.me = nrand()
	DPrintf5("This is client %v", ck.me)
	return ck
}

//
// fetch the current value for a key.
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

/*
func (ck *Clerk) Get(key string) string {
	//lock
	ck.snum = ck.snum + 1
	reply := GetReply{}
	leaderTry := ck.lastLeader
	for {
		args := &GetArgs{Key: key, Snum: ck.snum, Cid: ck.me}
		reply = GetReply{}
		DPrintf5("before Call %d", leaderTry)
		ok := ck.servers[leaderTry].Call("KVServer.Get", args, &reply)
		DPrintf5("after Call")
		// You will have to modify this function.
		//WrongLeader bool
		//Err         Err
		//Value       string
		if !ok || reply.WrongLeader {
			leaderTry = (leaderTry + 1) % len(ck.servers) //REM: random assign
			continue
		}

		if !reply.WrongLeader {
			//REM: possible reply.leader op in paper
			ck.lastLeader = leaderTry
			break
		}

	}
	return reply.Value
}
*/

func (ck *Clerk) Get(key string) string {
	//lock
	ck.snum = ck.snum + 1
	//reply := GetReply{}
	args := &GetArgs{Key: key, Snum: ck.snum, Cid: ck.me}
	reply := GetReply{}
	for {
		//
		reply = GetReply{}
		DPrintf5("before Call %d", ck.lastLeader)
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", args, &reply)
		DPrintf5("after Call")
		// You will have to modify this function.
		//WrongLeader bool
		//Err         Err
		//Value       string
		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers) //REM: random assign
			continue
		}

		if !reply.WrongLeader {
			//REM: possible reply.leader op in paper

			break
		}

	}

	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//lock
	ck.snum = ck.snum + 1
	reply := PutAppendReply{}
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Snum: ck.snum, Cid: ck.me}

	//leaderTry := ck.lastLeader
	for {
		//log.Printf("Leaderid: %d", ck.lastLeader)
		reply = PutAppendReply{}

		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", args, &reply)
		//WrongLeader bool
		//Err         Err
		DPrintf4("Cli %d PutAppend %v to server %d. Get %v, %v", ck.me, op, ck.lastLeader, ok, reply.WrongLeader)
		if !ok || reply.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers) //REM: random assign
			continue
		}

		if !reply.WrongLeader {
			//log.Printf("Success Leaderid: %d", ck.lastLeader)
			//REM: possible reply.leader op in paper
			//ck.lastLeader = leaderTry
			break
		}

	}
	//WrongLeader bool
	//Err         Err
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
