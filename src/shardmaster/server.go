package shardmaster

import (
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../util"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.

	configs   []Config         // indexed by config num
	dupeTable map[int64]dupeOp // duplicate table to prevent a client from resending a request when it has already been processed

	// map of indices to a list of listener channels to wait on for RPC responses
	waitTable map[int][]chan Op
}

type dupeOp struct {
	SeqNum int
	Conf   Config
}

type Op struct {
	// Your data here.
	OpType   string
	Servers  map[int][]string // Join
	GIDs     []int            // Leave
	Shard    int              // Move
	GID      int              // Move
	Num      int              // Query
	ClientId int64
	SeqNum   int

	Conf Config // for Query responses to attach their corresponding config
}

// helper functions for waitTable to operate as observer pattern i.e event listener
func (sm *ShardMaster) waitTableAdd(index int, ch chan Op) {
	if _, ok := sm.waitTable[index]; !ok {
		sm.waitTable[index] = []chan Op{ch}
	} else {
		sm.waitTable[index] = append(sm.waitTable[index], ch)
	}
}

func (sm *ShardMaster) waitTableRemove(index int) {
	if _, ok := sm.waitTable[index]; ok {
		delete(sm.waitTable, index)
	}
}

func (sm *ShardMaster) waitTableEmit(index int, op Op) {
	if _, ok := sm.waitTable[index]; ok {
		for _, ch := range sm.waitTable[index] {
			go func(c chan Op) {
				c <- op
			}(ch)
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	ch := sm.operationHelper(reply, Op{OpType: OpTypeJoin, Servers: args.Servers, ClientId: args.ClientId, SeqNum: args.SeqNum})

	if ch == nil {
		return
	}

	response := sm.waitForResponse(ch, OpTypeJoin) // response will have the value, if any

	if response.OpType == OpTypeJoin && reflect.DeepEqual(response.Servers, args.Servers) &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	ch := sm.operationHelper(reply, Op{OpType: OpTypeLeave, GIDs: args.GIDs, ClientId: args.ClientId, SeqNum: args.SeqNum})

	if ch == nil {
		return
	}

	response := sm.waitForResponse(ch, OpTypeLeave) // response will have the value, if any

	if response.OpType == OpTypeLeave && reflect.DeepEqual(response.GIDs, args.GIDs) &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	ch := sm.operationHelper(reply, Op{OpType: OpTypeMove, Shard: args.Shard, GID: args.GID, ClientId: args.ClientId, SeqNum: args.SeqNum})

	if ch == nil {
		return
	}

	response := sm.waitForResponse(ch, OpTypeMove) // response will have the value, if any

	if response.OpType == OpTypeMove && response.Shard == args.Shard && response.GID == args.GID &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	ch := sm.operationHelper(reply, Op{OpType: OpTypeQuery, Num: args.Num, ClientId: args.ClientId, SeqNum: args.SeqNum})

	if ch == nil {
		return
	}

	response := sm.waitForResponse(ch, OpTypeQuery) // response will have the value, if any

	if response.OpType == OpTypeQuery && response.Num == args.Num &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		reply.WrongLeader = false
		reply.Config = response.Conf
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) operationHelper(reply interface{}, op Op) chan Op {
	sm.mu.Lock()
	util.Debug(util.DTrace, "SM%d operationHelper: %v", sm.me, op)

	if clientOp, ok := sm.dupeTable[op.ClientId]; ok {
		// check if the request has already been executed
		if clientOp.SeqNum == op.SeqNum {
			switch op.OpType {
			case OpTypeJoin:
				reply.(*JoinReply).WrongLeader = false
			case OpTypeLeave:
				reply.(*LeaveReply).WrongLeader = false
			case OpTypeMove:
				reply.(*MoveReply).WrongLeader = false
			case OpTypeQuery:
				reply.(*QueryReply).WrongLeader = false
				reply.(*QueryReply).Config = clientOp.Conf
			}

			sm.mu.Unlock()
			return nil
		}
	}

	index, _, isLeader := sm.rf.Start(op)

	if !isLeader {
		switch op.OpType {
		case OpTypeJoin:
			reply.(*JoinReply).WrongLeader = true
		case OpTypeLeave:
			reply.(*LeaveReply).WrongLeader = true
		case OpTypeMove:
			reply.(*MoveReply).WrongLeader = true
		case OpTypeQuery:
			reply.(*QueryReply).WrongLeader = true
		}
		sm.mu.Unlock()
		return nil
	}

	// add to waitTable
	ch := make(chan Op)
	sm.waitTableAdd(index, ch)

	sm.mu.Unlock()
	return ch
}

func (sm *ShardMaster) waitForResponse(ch chan Op, opType string) Op {
	// wait for the response
	for {
		select {
		case response := <-ch:
			util.Debug(util.DTrace, "SM%d %s response received: %+v", sm.me, opType, response)
			return response
		case <-time.After(raft.ElectionTimeoutMin * time.Millisecond):
			// are we still the leader?
			if _, isLeader := sm.rf.GetState(); !isLeader {
				util.Debug(util.DTrace, "SM%d %s timed out\n", sm.me, opType)
				return Op{}
			}
		}
	}
}

func (sm *ShardMaster) apply() {
	for {
		if sm.killed() {
			return
		}

		applyMsg := <-sm.applyCh

		sm.mu.Lock()

		// distribute to listeners
		if applyMsg.CommandValid {

			op := applyMsg.Command.(Op)
			util.Debug(util.DTrace, "SM%d kv apply: %+v", sm.me, applyMsg)
			// do not re-execute if the operation is a duplicate
			clientOp, ok := sm.dupeTable[op.ClientId]

			if ok && clientOp.SeqNum == op.SeqNum {
				// do not update Configs and dupeTable for duplicate operations
				// however, we still need to provide a value to the client for Query(),
				// in case the first applied Query() was already emitted to a timed out Query channel
				if op.OpType == OpTypeQuery {
					op.Conf = clientOp.Conf
				}
			} else {
				sm.applyOp(&op)
			}

			sm.waitTableEmit(applyMsg.CommandIndex, op)
			sm.waitTableRemove(applyMsg.CommandIndex)
		}

		sm.mu.Unlock()
	}
}

// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
func (sm *ShardMaster) applyOp(op *Op) {
	switch op.OpType {
	case OpTypeJoin:
		newConfigShards := sm.configs[len(sm.configs)-1].Shards
		newConfigGroups := make(map[int][]string, len(sm.configs[len(sm.configs)-1].Groups))
		for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
			newConfigGroups[gid] = servers
		}

		// add new gid -> servers mappings
		for gid, servers := range op.Servers {
			newConfigGroups[gid] = servers
		}

		sm.rebalanceShards(&newConfigShards, newConfigGroups)

		newConfig := Config{}
		newConfig.Num = len(sm.configs)
		newConfig.Shards = newConfigShards
		newConfig.Groups = newConfigGroups
		sm.configs = append(sm.configs, newConfig)
		sm.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum}
	case OpTypeLeave:
		newConfigShards := sm.configs[len(sm.configs)-1].Shards
		newConfigGroups := make(map[int][]string, len(sm.configs[len(sm.configs)-1].Groups))
		for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
			newConfigGroups[gid] = servers
		}

		for _, gid := range op.GIDs {
			delete(newConfigGroups, gid)
		}

		sm.rebalanceShards(&newConfigShards, newConfigGroups)

		newConfig := Config{}
		newConfig.Num = len(sm.configs)
		newConfig.Shards = newConfigShards
		newConfig.Groups = newConfigGroups
		sm.configs = append(sm.configs, newConfig)
		sm.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum}
	case OpTypeMove:
		newConfigShards := sm.configs[len(sm.configs)-1].Shards
		newConfigShards[op.Shard] = op.GID
		newConfigGroups := make(map[int][]string, len(sm.configs[len(sm.configs)-1].Groups))
		for gid, servers := range sm.configs[len(sm.configs)-1].Groups {
			newConfigGroups[gid] = servers
		}
		newConfig := Config{}
		newConfig.Num = len(sm.configs)
		newConfig.Shards = newConfigShards
		newConfig.Groups = newConfigGroups
		sm.configs = append(sm.configs, newConfig)
		sm.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum}
	case OpTypeQuery:
		if op.Num == -1 || op.Num >= len(sm.configs) {
			op.Conf = sm.configs[len(sm.configs)-1]
		} else {
			op.Conf = sm.configs[op.Num]
		}
		sm.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum, Conf: op.Conf}
	}

	util.Debug(util.DTrace, "SM%d applyOp: %+v", sm.me, sm.configs)
}

func (sm *ShardMaster) rebalanceShards(shards *[NShards]int, groups map[int][]string) {
	// we want to divide the shards as evenly as possible, and to move as few shards as possible
	if len(groups) == 0 {
		return
	}
	gidToShardCount := make(map[int]int)
	for gid, _ := range groups {
		gidToShardCount[gid] = 0
	}

	minShardsPerGID := NShards / len(groups)
	maxShardsPerGID := int(math.Ceil(float64(NShards) / float64(len(groups))))

	// count shards per gid and identify any shards that are newly unassigned
	for shard, gid := range shards {
		if count, ok := gidToShardCount[gid]; ok && count < maxShardsPerGID {
			gidToShardCount[gid]++
		} else {
			shards[shard] = 0
		}
	}

	// assign all unassigned shards before we move shards from one gid to another
	for shard, gid := range shards {
		if gid == 0 {
			for newGID, count := range gidToShardCount {
				if count < minShardsPerGID {
					shards[shard] = newGID
					gidToShardCount[newGID]++
					break
				}
			}
		}
	}

	// move shards from GIDs that have more than minShardsPerGID to GIDs that still have less than minShardsPerGID
	// this is to ensure that the shards are evenly distributed
	for gid, count := range gidToShardCount {
		if count < minShardsPerGID {
			for shard, oldGID := range shards {
				if gidToShardCount[oldGID] > minShardsPerGID {
					shards[shard] = gid
					gidToShardCount[gid]++
					gidToShardCount[oldGID]--
					if gidToShardCount[gid] >= minShardsPerGID {
						break
					}
				}
			}
		}
	}

	util.Debug(util.DTrace, "SM%d rebalanceShards, shards: %+v, groups: %+v", sm.me, shards, groups)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Num = 0
	sm.configs[0].Shards = [NShards]int{}
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.dupeTable = make(map[int64]dupeOp)
	sm.waitTable = make(map[int][]chan Op)

	go sm.apply()

	return sm
}
