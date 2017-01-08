package raftkv

import (
	"encoding/gob"
	"labrpc"
	//"log"
	"raft"
	"sync"
	"bytes"
	// "crypto/rand"
	// "math/big"
	"time"
)

import (
  log "github.com/Sirupsen/logrus"
)

func init() {
  // Only log the warning severity or above.
  // log.SetLevel(log.InfoLevel)
  // log.SetLevel(log.DebugLevel)
  // log.SetLevel(log.WarnLevel)
  log.SetLevel(log.ErrorLevel)
  // log.SetLevel(log.FatalLevel)
}

// Operation Enum
const (
	GET = 0
	PUT = 1
	APPEND = 2
)

const Debug = 0
const SHUTDOWN_CHANNEL_TIMEOUT = 100

func assert(is_true bool, message string) {
    if !is_true {
        panic(message)
    }
}

// Command for raft log
type Op struct {

	UniqueClientID	int64 // from nrand()
	ClientCommandNumber int
	OperationType		int // 'get' 'append' 'put'
	Key			string
	Value 		string // '' if it's a get
}

// Capitalized because we need to encode
type CommandNumberAndValue struct {

	CommandNumber 	int
	Value 	string
}

type ClientIDTermChannel struct {
	UniqueClientID int64
	Term 	int
	CommandShutdownChannel chan ShutdownValue
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Shutdown channel that corresponds to the gorountine that you start that tries to commit the command to log
	// The apply channel loop sends to this shutdown channel when it gets something from apply channel that matches an index
	//clientCommandShutdownChannelMap			map[CommandIndexTerm]chan ShutdownValue
	clientCommandShutdownChannelMap			map[int][]ClientIDTermChannel


	KvDict	map[string]string

	// Client to the highest request seen from apply channel and corresponding value
	LargestSeenClientCommandMap		map[int64]CommandNumberAndValue

	HighestIndexSeen	int
	HighestTermSeen		int 

	kvraftShutdownChannel	chan int
}

type SnapshotIndexAndTerm struct {

	Index	int
	Term	int
}

type ShutdownValue struct {

	ClientReturnValue string
	Succeeded	bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {

	//Call Start with the Op struct as the command argument
	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: GET, Key: args.Key, Value: ""}
	
	kv.mu.Lock() 
	index, term, isLeader := kv.rf.Start(commandToLog)

	if isLeader {	

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//kv.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand
		
		// Create the shutdown channel struct
		// Check if the index has something there
		// If it does
			// Just append the new struct to the slice
		// If not
			// Make a new slice with that one element in it
		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := kv.clientCommandShutdownChannelMap[index]

		if inMap {
			kv.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			kv.clientCommandShutdownChannelMap[index] = newSlice
		}

		kv.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			kv.mu.Lock()
			kv.deleteCommandChannel(index, args.ClientIDNumber)
			kv.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				reply.Value = shutdownValue.ClientReturnValue // should be non null string
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		kv.mu.Unlock()		
		reply.WrongLeader = true
	}

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	//Call Start with the Op struct as the command argument, with appropriate operation type
	opType := PUT
	if args.Op == "Append" {
		opType = APPEND
	}
	commandToLog := Op{UniqueClientID: args.ClientIDNumber, ClientCommandNumber: args.CommandNumber, OperationType: opType, Key: args.Key, Value: args.Value}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(commandToLog)

	if isLeader {

		// Create a shutdown channel and add that and the Op struct
		shutdownChannelForCommand := make(chan ShutdownValue)
		//commandInfo := CommandIndexTerm{Command: commandToLog, Index: index, Term: term}
		//kv.clientCommandShutdownChannelMap[commandInfo] = shutdownChannelForCommand

		commandShutdown := ClientIDTermChannel{UniqueClientID: args.ClientIDNumber, Term: term, CommandShutdownChannel: shutdownChannelForCommand}
		currentSlice, inMap := kv.clientCommandShutdownChannelMap[index]

		if inMap {
			kv.clientCommandShutdownChannelMap[index] = append(currentSlice, commandShutdown)
		} else {
			newSlice := []ClientIDTermChannel{commandShutdown}
			kv.clientCommandShutdownChannelMap[index] = newSlice
		}

		kv.mu.Unlock()

		timeout := time.NewTimer(time.Duration(SHUTDOWN_CHANNEL_TIMEOUT) * time.Millisecond)

		select {
		case <-timeout.C:

			reply.WrongLeader = true
			kv.mu.Lock()
			kv.deleteCommandChannel(index, args.ClientIDNumber)
			kv.mu.Unlock()
			return
		case shutdownValue := <-shutdownChannelForCommand:
			if shutdownValue.Succeeded {

				reply.WrongLeader = false
				return
			} else {
				reply.WrongLeader = true
				return
			}
		}
	} else {
		kv.mu.Unlock()
		reply.WrongLeader = true
	}
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {

	kv.rf.Kill()
	close(kv.kvraftShutdownChannel)
}

func (kv *RaftKV) persist(persister *raft.Persister) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.HighestIndexSeen) //The committed thing that causes the log to grow above threshold
	e.Encode(kv.HighestTermSeen)
	e.Encode(kv.KvDict)
	e.Encode(kv.LargestSeenClientCommandMap)
	data := w.Bytes()
	persister.SaveSnapshot(data)
}

func (kv *RaftKV) readPersist(data []byte) {

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.HighestIndexSeen)
	d.Decode(&kv.HighestTermSeen)				
	d.Decode(&kv.KvDict)
	d.Decode(&kv.LargestSeenClientCommandMap)
}				

func (kv *RaftKV) deleteCommandChannel(index int, clientID int64) {

	// Iterate through the slice mapped to by the provided index until you get to the ClientIDTermChannel struct that has a matching clientID
	// Keep track of which index of the slice you're on and remove from
	indexToDelete := -1

	for index, element := range kv.clientCommandShutdownChannelMap[index] {
		if element.UniqueClientID == clientID {
			indexToDelete = index
		}
	}

	// Two sources of deletion, one from timeout and one if another command arrived at that same index
	if indexToDelete != -1 {
		currentSlice := kv.clientCommandShutdownChannelMap[index]
		kv.clientCommandShutdownChannelMap[index] = append(currentSlice[:indexToDelete], currentSlice[indexToDelete+1:]...)
	}
}


//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.KvDict = make(map[string]string)
	kv.LargestSeenClientCommandMap = make(map[int64]CommandNumberAndValue)
	//kv.clientCommandShutdownChannelMap = make(map[CommandIndexTerm]chan ShutdownValue)
	kv.clientCommandShutdownChannelMap = make(map[int][]ClientIDTermChannel)

	kv.kvraftShutdownChannel = make(chan int)

	kv.readPersist(persister.ReadSnapshot())	

	// Only start this raft size checking go routine if necesssary
	if maxraftstate != -1 { 
		go func() {

			for {
				select{
				case <-kv.kvraftShutdownChannel:
					return
				default:
					kv.mu.Lock()
					IndexToSendInSnapshot := kv.HighestIndexSeen

					if persister.RaftStateSize() > maxraftstate {

						kv.persist(persister)

						// Let the raft know the index it can delete up to b/c encapsulated by snapshot
						kv.rf.TrimLogsViaSnapshot(IndexToSendInSnapshot-1)
						kv.mu.Unlock()
					} else {
						kv.mu.Unlock()
					}

					time.Sleep(time.Duration(200) * time.Millisecond)
				}
			}
		}()
	}

	// Start the goroutine that listens to the applyCh
	go func() {
		for {
			select{
			case <- kv.kvraftShutdownChannel:
				return
			case applyChannelMessage := <- kv.applyCh: // Listen to the apply channel constantly
				
				kv.mu.Lock() 

				if applyChannelMessage.UseSnapshot{

					//Only apply this if it is a higher index one
					if applyChannelMessage.Term >= kv.HighestTermSeen && applyChannelMessage.Index-1 >= kv.HighestIndexSeen {
						data := applyChannelMessage.Snapshot

						// Do this in raft?
						persister.SaveSnapshot(data)

						r := bytes.NewBuffer(data)
						d := gob.NewDecoder(r)
						d.Decode(&kv.HighestIndexSeen)
						d.Decode(&kv.HighestTermSeen)				
						d.Decode(&kv.KvDict)
						d.Decode(&kv.LargestSeenClientCommandMap)
					}

					kv.mu.Unlock()
					continue
				}

				currentCommand := applyChannelMessage.Command.(Op)
				clientID := currentCommand.UniqueClientID
				commandIndex := applyChannelMessage.Index
				currentTerm := kv.rf.CurrentTerm
				
				if currentCommand.ClientCommandNumber > kv.LargestSeenClientCommandMap[clientID].CommandNumber {

					// Once you get something, change the kv dictionary if OperationType is 'put' or 'append' and update the CommandNumberValue table
					if currentCommand.OperationType == PUT {
						kv.KvDict[currentCommand.Key] = currentCommand.Value

						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: ""}
						kv.LargestSeenClientCommandMap[clientID] = updateCNV

					} else if currentCommand.OperationType == APPEND {
						currentValue := kv.KvDict[currentCommand.Key] // '' if null
						currentValue += currentCommand.Value
						kv.KvDict[currentCommand.Key] = currentValue

						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: ""}
						kv.LargestSeenClientCommandMap[clientID] = updateCNV

					} else {

						updateCNV := CommandNumberAndValue{CommandNumber: currentCommand.ClientCommandNumber, Value: kv.KvDict[currentCommand.Key]}
						kv.LargestSeenClientCommandMap[clientID] = updateCNV
					}
					kv.HighestIndexSeen = applyChannelMessage.Index-1 // minus 1 because sent to raft
					kv.HighestTermSeen = applyChannelMessage.Term

					currentIndexChannels := kv.clientCommandShutdownChannelMap[commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)
							// Create appropriate value to shutdown channel
							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {
								toChannel.Succeeded = true
								if currentCommand.OperationType == GET {
									toChannel.ClientReturnValue = kv.KvDict[currentCommand.Key] 
								}
							}
							select{
							case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
							default:
							}							
							break
						}
					}
					//Delete from table
					kv.deleteCommandChannel(clientCommandChannelIndex, clientID)

				} else if currentCommand.ClientCommandNumber == kv.LargestSeenClientCommandMap[clientID].CommandNumber {

					currentIndexChannels := kv.clientCommandShutdownChannelMap[commandIndex]

					clientCommandChannelIndex := -1

					for index, clientIDTermChannel := range currentIndexChannels {
						if clientIDTermChannel.UniqueClientID == clientID {
							clientCommandChannelIndex = index

							toChannel := new(ShutdownValue)
							// Create appropriate value to shutdown channel
							if clientIDTermChannel.Term < currentTerm {
								toChannel.Succeeded = false
							} else {
								toChannel.Succeeded = true
								toChannel.ClientReturnValue = kv.LargestSeenClientCommandMap[clientID].Value
								
							}
							select{
							case clientIDTermChannel.CommandShutdownChannel<-*toChannel:
							default:
							}							
							break
						}
					}
					//Delete from table
					kv.deleteCommandChannel(clientCommandChannelIndex, clientID)
				}

				kv.mu.Unlock()
			}
		}
	}()	

	return kv
}
