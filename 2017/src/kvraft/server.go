package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//定义Op结构体
type Op struct {
	Oprand string      //随机操作ID
	Args   interface{} //请求类型s
}

type DBEntry struct {
	Command Op
	Exist   bool
	Value   string
}

type RaftKV struct {
	mu      sync.Mutex //锁保护
	me      int        //在端点数组中的索引
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // maxraftstate用于表示log的最大长度

	kvstore map[string]string    //表示存储key/value
	history map[int64]int        //记录每个节点最后一个操作的ID
	result  map[int]chan DBEntry //记录正在执行的操作
	done    chan bool            //判断还是否存活
}

//复制请求
func (kv *RaftKV) DuplicateReq(cid int64, sid int) bool {
	value, ok := kv.history[cid]
	if !ok {
		return false
	} else {
		// return true
		return value >= sid
	}
}

//返回值
func (kv *RaftKV) ReturnValue(key string, value string, op string) string {
	if op == "Put" {
		return value
	} else {
		return kv.kvstore[key] + value
	}
}

func (kv *RaftKV) CheckSnapshot(idx int) {
	if kv.maxraftstate != -1 && float64(kv.rf.GetPersistSize()) > float64(kv.maxraftstate)*0.8 {
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(kv.kvstore)
		e.Encode(kv.history)
		data := w.Bytes()

		go kv.rf.StartSnapshot(data, idx)
	}
}

//当收到来自客户端的请求时，用Apply函数判断是什么操作，并进一步调用其他函数。
func (kv *RaftKV) Apply(args Op) {
	if args.Oprand == "Get" {
		// record history
		getArgs := args.Args.(GetArgs)
		kv.history[getArgs.Cid] = getArgs.Sid
	} else {
		putAppendArgs := args.Args.(PutAppendArgs)
		kv.history[putAppendArgs.Cid] = putAppendArgs.Sid
		kv.kvstore[putAppendArgs.Key] = kv.ReturnValue(putAppendArgs.Key, putAppendArgs.Value, args.Oprand)
	}
}

func (kv *RaftKV) Run(value Op) (bool, DBEntry) {
	// wait till idx arrives at appropriate time
	var entry DBEntry
	idx, _, isLeader := kv.rf.Start(value)

	if !isLeader {
		return false, entry
	}

	kv.mu.Lock()

	ch, exist := kv.result[idx]

	if !exist {
		kv.result[idx] = make(chan DBEntry, 1)
		ch = kv.result[idx]
	}

	kv.mu.Unlock()

	select {
	case entry = <-ch:
		return entry.Command == value, entry
	case <-time.After(1000 * time.Millisecond):
		return false, entry
	}
}

//先封装这次操作请求的请求参数，然后调用Leader服务节点的RPC请求。
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	value := Op{Oprand: "Get", Args: *args}
	ok, entry := kv.Run(value)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		if entry.Exist {
			reply.Err = OK
			reply.Value = entry.Value
		} else {
			reply.Err = ErrNoKey
		}
	}
	return
}

//先封装这次操作请求的请求参数，然后调用Leader服务节点的RPC请求。
func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	value := Op{Oprand: args.Op, Args: *args}
	ok, _ := kv.Run(value)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	return
}

func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

func (kv *RaftKV) shutdown() bool {
	select {
	case <-kv.done:
		return true
	default:
		return false
	}
}

//初始化1个kvraft节点，涉及的结构体为RaftKV结构体即描述kvraft节点
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.done = make(chan bool)

	kv.history = make(map[int64]int)
	kv.kvstore = make(map[string]string)
	kv.result = make(map[int]chan DBEntry)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for {

			if kv.shutdown() {
				return
			}
			// retrieve msg from channel
			applyMsg := <-kv.applyCh

			if applyMsg.UseSnapshot {
				var LastIncludedIndex int
				var LastIncludedTerm int

				r := bytes.NewBuffer(applyMsg.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()
				d.Decode(&LastIncludedIndex)
				d.Decode(&LastIncludedTerm)
				kv.kvstore = make(map[string]string)
				kv.history = make(map[int64]int)
				d.Decode(&kv.kvstore)
				d.Decode(&kv.history)
				kv.mu.Unlock()
			} else {

				op := applyMsg.Command.(Op)

				var machine int64
				var req int
				var key string
				var entry DBEntry
				index := applyMsg.Index

				if op.Oprand == "Get" {
					getArgs := op.Args.(GetArgs)
					machine = getArgs.Cid
					req = getArgs.Sid
					key = getArgs.Key
				} else {
					putAppendArgs := op.Args.(PutAppendArgs)
					machine = putAppendArgs.Cid
					req = putAppendArgs.Sid
				}

				kv.mu.Lock()

				if !kv.DuplicateReq(machine, req) {
					kv.Apply(op)
				}

				entry.Command = op

				if op.Oprand == "Get" {
					v, exist := kv.kvstore[key]
					entry.Value = v
					entry.Exist = exist
				}

				_, exist := kv.result[index]

				if !exist {
					kv.result[index] = make(chan DBEntry, 1)
				} else {
					// clear the channel
					select {
					case <-kv.result[index]:
					default:
					}
				}

				kv.result[index] <- entry
				kv.CheckSnapshot(applyMsg.Index)
				kv.mu.Unlock()

			}
		}
	}()

	return kv
}
