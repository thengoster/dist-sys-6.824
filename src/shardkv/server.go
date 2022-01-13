package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string // empty string for Get
	OpType   string
	ClientId int64
	SeqNum   int
	Shard    int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	smClerk *shardmaster.Clerk
	config  shardmaster.Config

	// snapshot includes the following two maps:
	kvStore   map[string]string // the actual key-value store that we are implementing
	dupeTable map[int64]dupeOp  // duplicate table to prevent a client from resending a request when it has already been processed

	// map of indices to a list of listener channels to wait on for RPC responses
	waitTable map[int][]chan Op

	// include in the snapshot itself, do not rely on the raft state, since it is possible that the state
	// and accompanying snapshot are not consistent
	// (persister.go saves the raft state, but crashes before saving the snapshot)
	lastIncludedIndex int
	lastIncludedTerm  int
}

type dupeOp struct {
	SeqNum int
	Value  string
}

// helper functions for waitTable to operate as observer pattern i.e event listener
func (kv *ShardKV) waitTableAdd(index int, ch chan Op) {
	if _, ok := kv.waitTable[index]; !ok {
		kv.waitTable[index] = []chan Op{ch}
	} else {
		kv.waitTable[index] = append(kv.waitTable[index], ch)
	}
}

func (kv *ShardKV) waitTableRemove(index int) {
	if _, ok := kv.waitTable[index]; ok {
		delete(kv.waitTable, index)
	}
}

func (kv *ShardKV) waitTableEmit(index int, op Op) {
	if _, ok := kv.waitTable[index]; ok {
		for _, ch := range kv.waitTable[index] {
			go func(c chan Op) {
				c <- op
			}(ch)
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:      args.Key,
		OpType:   OpTypeGet,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Shard:    args.Shard,
	}
	ch := kv.operationHelper(reply, op)

	if ch == nil {
		return
	}

	response := kv.waitForResponse(ch, OpTypeGet) // response will have the value, if any

	if response.OpType == OpTypeGet && response.Key == args.Key &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		// search for the key in the kvStore
		if response.Value != "" {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		reply.Value = response.Value
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		Shard:    args.Shard,
	}
	ch := kv.operationHelper(reply, op)

	if ch == nil {
		return
	}

	response := kv.waitForResponse(ch, args.Op)

	if response.OpType == args.Op && response.Key == args.Key && response.Value == args.Value &&
		response.ClientId == args.ClientId && response.SeqNum == args.SeqNum {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) operationHelper(reply interface{}, op Op) chan Op {
	kv.mu.Lock()

	DPrintf("%d %s: %+v\n", kv.me, op.OpType, op)
	// check if we are in charge of this shard
	if kv.config.Shards[op.Shard] != kv.gid {
		if _, ok := reply.(*GetReply); ok {
			reply.(*GetReply).Err = ErrWrongGroup
		} else {
			reply.(*PutAppendReply).Err = ErrWrongGroup
		}
		kv.mu.Unlock()
		return nil
	}

	if clientOp, ok := kv.dupeTable[op.ClientId]; ok {
		// check if the request has already been executed
		if clientOp.SeqNum == op.SeqNum {
			if _, ok := reply.(*GetReply); ok {
				reply.(*GetReply).Err = OK
				reply.(*GetReply).Value = clientOp.Value
			} else {
				reply.(*PutAppendReply).Err = OK
			}

			kv.mu.Unlock()
			return nil
		}
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		if _, ok := reply.(*GetReply); ok {
			reply.(*GetReply).Err = ErrWrongLeader
		} else {
			reply.(*PutAppendReply).Err = ErrWrongLeader
		}
		kv.mu.Unlock()
		return nil
	}

	// add to waitTable
	ch := make(chan Op)
	kv.waitTableAdd(index, ch)

	kv.mu.Unlock()
	return ch
}

func (kv *ShardKV) waitForResponse(ch chan Op, opType string) Op {
	// wait for the response
	// DPrintf("%d %s: waiting for response\n", kv.me, opType)
	for {
		select {
		case response := <-ch:
			DPrintf("%d %s: response received: %+v\n", kv.me, opType, response)
			return response
		case <-time.After(raft.ElectionTimeoutMin * time.Millisecond):
			// are we still the leader?
			if _, isLeader := kv.rf.GetState(); !isLeader {
				// DPrintf("%d %s: timed out\n", kv.me, opType)
				return Op{}
			}
		}
	}
}

func (kv *ShardKV) apply() {
	for {
		applyMsg := <-kv.applyCh

		kv.mu.Lock()

		// distribute to listeners
		if applyMsg.CommandValid {

			op := applyMsg.Command.(Op)
			DPrintf("%d kv apply: %+v\n", kv.me, applyMsg)
			// do not re-execute if the operation is a duplicate
			clientOp, ok := kv.dupeTable[op.ClientId]

			if ok && clientOp.SeqNum == op.SeqNum {
				// do not update kvStore and dupeTable for duplicate operations
				// however, we still need to provide a value to the client for Get(),
				// in case the first applied Get() was already emitted to a timed out Get channel
				if op.OpType == OpTypeGet {
					if value, ok := kv.kvStore[op.Key]; ok {
						op.Value = value
					}
				}
			} else {
				kv.applyOp(&op)
			}

			kv.waitTableEmit(applyMsg.CommandIndex, op)
			kv.waitTableRemove(applyMsg.CommandIndex)

			// take a snapshot if the log is too big
			if kv.maxraftstate != -1 &&
				kv.lastIncludedIndex < applyMsg.CommandIndex &&
				kv.rf.GetRaftStateSize()/float64(kv.maxraftstate) > 0.8 {
				// DPrintf("%d kv apply: snapshotting\n", kv.me)
				kv.lastIncludedIndex = applyMsg.CommandIndex
				kv.lastIncludedTerm = applyMsg.CommandTerm
				snapshot := kv.encodeSnapshot()
				kv.rf.PersistStateAndSnapshotWithLock(snapshot, applyMsg.CommandIndex, applyMsg.CommandTerm)
			}
		} else {
			DPrintf("%d kv apply: snapshot received\n", kv.me)
			kv.readPersistedSnapshot(applyMsg.Snapshot)
		}

		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	switch op.OpType {
	case OpTypeGet:
		if value, ok := kv.kvStore[op.Key]; ok {
			op.Value = value
			// update the duplicate table to mark as executed
			kv.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum, Value: value}
		}
	case OpTypePut:
		kv.kvStore[op.Key] = op.Value
		kv.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum}
	case OpTypeAppend:
		// DPrintf("%d kv append before: key: %s, value: %s\n", kv.me, op.Key, kv.kvStore[op.Key])
		kv.kvStore[op.Key] += op.Value
		DPrintf("%d kv append after: key: %s, value: %s\n", kv.me, op.Key, kv.kvStore[op.Key])
		kv.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// restore previously persisted snapshot.
//
func (kv *ShardKV) readPersistedSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		kv.kvStore = make(map[string]string)
		kv.dupeTable = make(map[int64]dupeOp)
		kv.lastIncludedIndex = 0
		kv.lastIncludedTerm = 0
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
		DPrintf("%d: readPersistedSnapshot() failed\n", kv.me)
		kv.kvStore = make(map[string]string)
		kv.dupeTable = make(map[int64]dupeOp)
		kv.lastIncludedIndex = 0
		kv.lastIncludedTerm = 0
	} else {
		kv.kvStore = kvStore
		kv.dupeTable = dupeTable
		kv.lastIncludedIndex = lastIncludedIndex
		kv.lastIncludedTerm = lastIncludedTerm
	}
}

func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.dupeTable)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	return w.Bytes()
}

// TODO
func (kv *ShardKV) pollConfigChange() {
	// for {
	// 	kv.mu.Lock()
	// 	// ask master for the latest configuration.
	// 	// currentConfig := kv.smClerk.Query(-1)
	// 	kv.config = kv.smClerk.Query(-1)

	// 	// evaluate the shards that our GID is/is not handling
	// 	kv.mu.Unlock()
	// 	time.Sleep(100 * time.Millisecond)
	// }
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("%d: starting kv server\n", me)

	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.smClerk = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersistedSnapshot(persister.ReadSnapshot())
	kv.waitTable = make(map[int][]chan Op)

	go kv.apply()
	go kv.pollConfigChange()

	return kv
}
