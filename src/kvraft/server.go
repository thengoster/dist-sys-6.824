package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

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
	OpType   string
	Key      string
	Value    string // empty string for Get
	ClientId int64
	SeqNum   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
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
func (kv *KVServer) waitTableAdd(index int, ch chan Op) {
	if _, ok := kv.waitTable[index]; !ok {
		kv.waitTable[index] = []chan Op{ch}
	} else {
		kv.waitTable[index] = append(kv.waitTable[index], ch)
	}
}

func (kv *KVServer) waitTableRemove(index int) {
	if _, ok := kv.waitTable[index]; ok {
		delete(kv.waitTable, index)
	}
}

func (kv *KVServer) waitTableEmit(index int, op Op) {
	if _, ok := kv.waitTable[index]; ok {
		for _, ch := range kv.waitTable[index] {
			go func(c chan Op) {
				c <- op
			}(ch)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("%d Get: %+v\n", kv.me, args)

	if clientOp, ok := kv.dupeTable[args.ClientId]; ok {
		// check if the request has already been executed
		if clientOp.SeqNum == args.SeqNum {
			reply.Err = OK
			reply.Value = clientOp.Value
			kv.mu.Unlock()
			return
		}
	}

	op := Op{
		OpType:   OpTypeGet,
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// add to waitTable
	ch := make(chan Op)
	kv.waitTableAdd(index, ch)

	kv.mu.Unlock()

	response := kv.waitForResponse(ch, op.OpType) // response will have the value, if any

	if response.OpType == op.OpType && response.Key == op.Key &&
		response.ClientId == op.ClientId && response.SeqNum == op.SeqNum {
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

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	DPrintf("%d PutAppend: %+v\n", kv.me, args)

	if clientOp, ok := kv.dupeTable[args.ClientId]; ok {
		// check if the request has already been executed
		if clientOp.SeqNum == args.SeqNum {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}

	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// add to waitTable
	ch := make(chan Op)
	kv.waitTableAdd(index, ch)

	kv.mu.Unlock()

	response := kv.waitForResponse(ch, op.OpType)

	if response.OpType == op.OpType && response.Key == op.Key && response.Value == op.Value &&
		response.ClientId == op.ClientId && response.SeqNum == op.SeqNum {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) waitForResponse(ch chan Op, opType string) Op {
	// wait for the response
	// DPrintf("%d %s: waiting for response\n", kv.me, opType)
	for {
		select {
		case response := <-ch:
			DPrintf("%d %s: response received\n", kv.me, opType)
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

func (kv *KVServer) apply() {
	for {
		if kv.killed() {
			return
		}

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
			// if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize()/float64(kv.maxraftstate) > 0.8 {
			if kv.maxraftstate != -1 &&
				kv.lastIncludedIndex < applyMsg.CommandIndex {
				// kv.lastIncludedIndex < applyMsg.CommandIndex &&
				// kv.rf.GetRaftStateSize()/float64(kv.maxraftstate) > 0.8 {
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

func (kv *KVServer) applyOp(op *Op) {
	switch op.OpType {
	case OpTypeGet:
		if value, ok := kv.kvStore[op.Key]; ok {
			// update the duplicate table to mark as executed
			kv.dupeTable[op.ClientId] = dupeOp{SeqNum: op.SeqNum, Value: value}
			op.Value = value
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
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// restore previously persisted snapshot.
//
func (kv *KVServer) readPersistedSnapshot(snapshot []byte) {
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

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvStore)
	e.Encode(kv.dupeTable)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.lastIncludedTerm)
	return w.Bytes()
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
	DPrintf("%d: starting kv server\n", me)

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersistedSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	kv.waitTable = make(map[int][]chan Op)

	go kv.apply()

	return kv
}
