package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

const (
	Wait = 100 * time.Millisecond
)

//客户端结构体
type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	sid int   // 储存id随机值
	id  int64 // 客户端ID

}

//生成随机数
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//初始客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.sid = 0
	ck.id = nrand() //客户端Id，在初始化函数中使用nrand()函数来生成随机值
	return ck
}

//Get方法。先封装这次操作的请求参数，然后循环调用领导者节点的RPC请求，
//如果返回结果为真则跳出循环，如果返回结果为假则表示该节点不是领导者，继续寻找下一个节点。
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{Key: key, Cid: ck.id, Sid: ck.sid}
	ck.sid++

	// outer for to make sure iteration till leader elected
	for {
		for _, server := range ck.servers {
			reply := GetReply{}
			ok := server.Call("RaftKV.Get", args, &reply)

			if !reply.WrongLeader && ok {
				if reply.Err == ErrNoKey {
					reply.Value = ""
				}

				return reply.Value
			}
		}

		time.Sleep(Wait)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Cid: ck.id, Sid: ck.sid}
	ck.sid++

	for {
		for _, server := range ck.servers {
			reply := PutAppendReply{}
			ok := server.Call("RaftKV.PutAppend", args, &reply)

			if !reply.WrongLeader && ok {
				return
			}
		}
		//time.Sleep(Wait)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
