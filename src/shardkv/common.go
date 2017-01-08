package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {

	Key   string
	Value string
	Op    string // "Put" or "Append"

	CommandNumber	int // Incrementally increasing
	ClientIDNumber	int64 // unique randID()
	Shard 	int 
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	CommandNumber	int // Incrementally increasing
	ClientIDNumber	int64 // unique randID()
	Shard 	int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type AcceptShardArgs struct {

	Shard 	int
	TransitionConfigNum 	int
	KvDict	map[string]string
	ClientCommandsSeen 	map[int64]CommandNumberAndValue
}

type AcceptShardReply struct {
	WrongLeader bool
	Err         Err	
}