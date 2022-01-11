package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const ClientRetryInterval = time.Duration(100) * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	// unique identifier for this client as well as monotonically increasing sequence number for each request,
	// to allow our shardmaster server to detect duplicate requests
	clientId      int64
	seqNum        int
	currentLeader int // cache the current shardmaster server leader
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	ck.currentLeader = 0
	return ck
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqNum++
	args := JoinArgs{}

	args.Servers = servers
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		reply := JoinReply{}
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Join", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		} else {
			// find the new leader
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}

		time.Sleep(ClientRetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqNum++
	args := LeaveArgs{}

	args.GIDs = gids
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Leave", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		} else {
			// find the new leader
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}

		time.Sleep(ClientRetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqNum++
	args := MoveArgs{}

	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		reply := MoveReply{}
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Move", &args, &reply)
		if ok && reply.WrongLeader == false {
			return
		} else {
			// find the new leader
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}

		time.Sleep(ClientRetryInterval)
	}
}

func (ck *Clerk) Query(num int) Config {
	ck.seqNum++
	args := QueryArgs{}

	args.Num = num
	args.ClientId = ck.clientId
	args.SeqNum = ck.seqNum

	for {
		reply := QueryReply{}
		ok := ck.servers[ck.currentLeader].Call("ShardMaster.Query", &args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		} else {
			// find the new leader
			ck.currentLeader = (ck.currentLeader + 1) % len(ck.servers)
		}

		time.Sleep(ClientRetryInterval)
	}
}
