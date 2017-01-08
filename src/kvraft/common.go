package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)
type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"


	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandNumber	int // Incrementally increasing
	ClientIDNumber	int64 // unique randID() 
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	CommandNumber	int // Incrementally increasing
	ClientIDNumber	int64 // unique randID() 
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
