package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// 定义PutAppend请求结构体
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"//操作类型
	Cid   int64  // 客户端ID
	Sid   int    // 操作ID

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

//定义PutAppend回复结构体
type PutAppendReply struct {
	WrongLeader bool //该服务节点是否是Leader
	Err         Err  //错误原因
}

//定义Get请求结构体
type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64 // 客户端ID
	Sid int   // 现在的操作ID
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
