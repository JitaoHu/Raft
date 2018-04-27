package raft

//通过将对象各属性进行编码储存，等待需要恢复时再解码并读取。
//一个基于Raft的服务器必须能够储存它停止时的状态，如果计算机宕机后重新启动，可以根据储存的状态恢复到宕机之前的状态。

import "sync"

//定义持久化对象各项属性
type Persister struct {
	mu        sync.Mutex //锁保护
	raftstate []byte     //Raft状态值
	snapshot  []byte
}

//创建持久化对象
func MakePersister() *Persister {
	return &Persister{}
}

//拷贝持久化对象
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

//保存数据化到持久化对象
func (ps *Persister) SaveRaftState(data []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = data
}

//获取持久化对象
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

//获取Raft状态数据的大小
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}
