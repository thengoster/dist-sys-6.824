package kvraft

import (
	"crypto/rand"
	"math/big"

	"time"

	"../labrpc"
)

const ClientRetryInterval = time.Duration(100) * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// unique identifier for this client as well as monotonically increasing sequence number for each request
	clientId      int64
	seqNum        int
	currentLeader int // cache the current kv server leader
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
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.currentLeader = 0 // this is just an init guess for the leader, will update as we apply RPCs
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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqNum++
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqNum: ck.seqNum}

	for {
		reply := GetReply{}
		// DPrintf("[Clerk](%d) Get to [%v]", ck.clientId, ck.currentLeader)
		ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				// DPrintf("[Clerk](%d) Get args/reply from %v: args: %+v\nreply: %+v\n", ck.clientId, ck.currentLeader, args, reply)
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			} else if reply.Err == ErrNoKey {
				return ""
			}
		} else { // stalled RPC due to network issue, try on next server
			// DPrintf("[Clerk](%d) Get timed out from %v\n", ck.clientId, ck.currentLeader)
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}
		time.Sleep(ClientRetryInterval)
	}
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
	ck.seqNum++
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqNum: ck.seqNum}

	for {
		reply := PutAppendReply{}
		// DPrintf("[Clerk](%d) PutAppend to [%v], value: %v", ck.clientId, ck.currentLeader, value)
		ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				// DPrintf("[Clerk](%d) PutAppend args/reply from %v: args: %+v\nreply: %+v\n", ck.clientId, ck.currentLeader, args, reply)
				return
			} else if reply.Err == ErrWrongLeader {
				ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
			}
		} else { // stalled RPC due to network issue, try on next server
			// DPrintf("[Clerk](%d) PutAppend timed out from %v\n", ck.clientId, ck.currentLeader)
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}
		time.Sleep(ClientRetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
