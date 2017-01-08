package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server. 
//

import "sync"
import "labrpc"
import "time" 
import "math/rand"
import "bytes"
import "encoding/gob"

import (
  log "github.com/Sirupsen/logrus"
)

const HEARTBEAT_TIMEOUT int = 50
const ELECTION_TIMEOUT_BASE int = 200
const ELECTION_TIMEOUT_VARIANCE int = 200
const VOTES_TIMEOUT int = 20
const RPC_SENDING_TIMEOUT int = 20
const LOOP_SLEEP_AMOUNT int = 5
const TIMES_TO_RETRY_RPC int = 4

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2
)


func init() {
  // Only log the warning severity or above.
  // log.SetLevel(log.InfoLevel)
  // log.SetLevel(log.DebugLevel)
  // log.SetLevel(log.WarnLevel)
  log.SetLevel(log.ErrorLevel)
  // log.SetLevel(log.FatalLevel)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Term 		int
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	applyChannel	chan ApplyMsg
	votesChannel	chan *RequestVoteReply

	CurrentTerm		int
	VotedFor		int
	Log			[]LogEntry
	
	commitIndex		int
	lastApplied		int

	currentState 	int // Follower, Candidate, or Leader
	receivedVotes 	int

	electionTimer   *time.Timer
	heartbeatTimer  *time.Timer

	nextIndex		[]int // Leader value
	matchIndex 		[]int // Leader value

	// 3B Snapshot changes
	LastIndexIncluded	int // NLast index of all logs that have been snapshotted (and thus deleted)
	LastTermIncluded	int // Term of the log entry at lastIndexIncluded


	shutdownChannel		chan int
}


type LogEntry struct {
	Term		int
	Command 	interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.CurrentTerm, rf.currentState == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastIndexIncluded)
	e.Encode(rf.LastTermIncluded)
	//e.Encode(rf.LastTermIncluded)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Log)
	d.Decode(&rf.LastIndexIncluded)
	d.Decode(&rf.LastTermIncluded)
	//d.Decode(&rf.commitIndex)
}

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

func Max(x, y int) int {
    if x > y {
        return x
    }
    return y
}

//Changed votedFor to -1 here because we only reset that when we increase term
func (rf *Raft) increaseTermTo(newTerm int) {

	rf.CurrentTerm = newTerm
	//When we increase term we will always have 0 votes in that term and not have voted for anyjuan
	rf.receivedVotes = 0
	rf.VotedFor = -1
	rf.currentState = FOLLOWER // Extra safety test ig

	rf.persist()
}

func (rf *Raft) followerToCandidate() {

	rf.currentState = CANDIDATE
	rf.VotedFor = rf.me
	rf.receivedVotes = 1
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.resetElectionTimer()

	rf.persist()
}

func (rf *Raft) candidateToCandidate() {

	rf.receivedVotes = 1
	rf.VotedFor = rf.me
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.resetElectionTimer()

	rf.persist()
}

func (rf *Raft) candidateToLeader() {

	rf.currentState = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i,_ := range rf.matchIndex {
		// This initialized to -1 because no entries can be assumed to start as replicated
		rf.matchIndex[i] = -1 
	}
	
	for i,_ := range rf.nextIndex {
		// This initialized to last log index + 1 because PrevLogIndex is 1 less than this
		rf.nextIndex[i] =  len(rf.Log)+rf.LastIndexIncluded+1
	}
}

func (rf *Raft) candidateToFollower() {

	rf.currentState = FOLLOWER
	rf.resetElectionTimer()
}

func (rf *Raft) leaderToFollower() {

	rf.currentState = FOLLOWER
	rf.resetElectionTimer()
}


func assert(is_true bool, message string) {
    if !is_true {
        panic(message)
    }
}

type RequestVoteArgs struct {
	Term 	     int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term         int
	VoteGranted  bool
	ElectionTerm int // The election term this vote corresponds to
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderID		int
	PrevLogIndex 	int
	PrevLogTerm		int
	Entries 		[]LogEntry 
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 			int
	LeadershipTerm	int
	Success 		bool
	ConflictingEntryTerm	int
	FirstIndexWithConflictingTerm	int
}

type InstallSnapshotArgs struct {
	Term 			int
	LeaderID		int
	LastIndexIncluded	int
	LastTermIncluded	int
	SnapshotData 		[]byte 
}

type InstallSnapshotReply struct {
	Term 			int
	LeadershipTerm	int
	Success 		bool
	FirstIndexWithConflictingTerm	int
// 	ConflictingEntryTerm	int
}

type SnapshotIndexAndTerm struct {
	Index	int
	Term	int
}


func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(time.Duration(ELECTION_TIMEOUT_BASE + rand.Intn(ELECTION_TIMEOUT_VARIANCE)) * time.Millisecond)
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Reset(time.Duration(HEARTBEAT_TIMEOUT) * time.Millisecond)
}

func (rf *Raft) trySendApplyMsgs(leaderCommitIndex int) {

	if leaderCommitIndex > rf.commitIndex {

		newCommitIndex := Min(leaderCommitIndex, len(rf.Log)+rf.LastIndexIncluded+1 - 1) //-1 for the last index of the log, (+rf.LastIndexIncluded+1 to account for offset)		
		rf.commitIndex = newCommitIndex
	}
}

//Takes care of appending and updating the reply message
func (rf *Raft) receiverAppendEntriesHandling(args AppendEntriesArgs, reply *AppendEntriesReply) {

	reply.ConflictingEntryTerm = -1
	reply.FirstIndexWithConflictingTerm = -1

	//We have a receiver with empty log so just append (or all entries are fucked)
	if args.PrevLogIndex == -1 {
		
		rf.Log = args.Entries
		rf.persist()

		rf.trySendApplyMsgs(args.LeaderCommit)		
		reply.Success = true

	} else {

		//No log entry there
		if len(rf.Log)+rf.LastIndexIncluded+1 <= args.PrevLogIndex {

			reply.FirstIndexWithConflictingTerm = len(rf.Log)+rf.LastIndexIncluded+1 //If they shorters beings us to their tip
			reply.ConflictingEntryTerm = -1
			reply.Success = false
			return
		}

		// Check to see if PrevLogIndex is less than lastIndexIncluded
		if args.PrevLogIndex < rf.LastIndexIncluded {
			reply.Success = false
			reply.FirstIndexWithConflictingTerm = rf.LastIndexIncluded + 1
			return
		} else if args.PrevLogIndex != rf.LastIndexIncluded { // If args.PrevLogIndex is greater than lastIndexIncluded we can do code from before 

			//Conflicting terms on same index so fail
			if rf.Log[args.PrevLogIndex-rf.LastIndexIncluded-1].Term != args.PrevLogTerm {

				conflictingTerm := rf.Log[args.PrevLogIndex-rf.LastIndexIncluded-1].Term

				// 3-14 change truncating
				rf.Log = rf.Log[:args.PrevLogIndex-rf.LastIndexIncluded-1] //Truncate the log if there is a conflict
				rf.persist()

				conflictingIndex := args.PrevLogIndex

				// Find the first entry from that terms
				for conflictingIndex-1-rf.LastIndexIncluded-1 > 0 { // This is atrocious but basically just scans until end of log				

					if rf.Log[conflictingIndex-1-rf.LastIndexIncluded-1].Term == conflictingTerm {
						conflictingIndex--
					} else {
						break
					}
				}

				reply.ConflictingEntryTerm = conflictingTerm
				reply.FirstIndexWithConflictingTerm = conflictingIndex 
				reply.Success = false

				return
			}
		} else { // args.PrevLogIndex == rf.LastIndexIncluded, both this stuff is committed so we know it has same term so just move on

			// In this case we know there is an element with matching term/index
		}

		// At this point we know there is a matching Term element at PrevLogIndex so
		// iterate through until we figure out how many entries we need to append.
		// Check term at each Entry if it equal just ignore it

		appendNewEntriesFrom := -1 // If this remains negative 1 then don't need to append anything
		followerLogIndexToAppendTo := -1

		for i := 0; i < len(args.Entries); i++ {

			followerLogIndexToAppendTo = args.PrevLogIndex + 1 + i

			if followerLogIndexToAppendTo >= len(rf.Log)+rf.LastIndexIncluded+1 {
				// We are past the existing log so append from this i
				appendNewEntriesFrom = i
				break
			} else {
				// Check term to make sure it same
				if rf.Log[followerLogIndexToAppendTo-rf.LastIndexIncluded-1].Term != args.Entries[i].Term {
					appendNewEntriesFrom = i
					break
				}
			}
		}

		if appendNewEntriesFrom != -1 {
			// We have stuff to append
			rf.Log = append(rf.Log[:followerLogIndexToAppendTo-rf.LastIndexIncluded-1], args.Entries[appendNewEntriesFrom:]...)
		}
		rf.persist()

		//Only accept the commit index AFTER accepting appendentries
		rf.trySendApplyMsgs(args.LeaderCommit)
		reply.Success = true
		return
	}
}

func (rf *Raft) HandleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()

	reply.LeadershipTerm = args.Term

	if args.Term < rf.CurrentTerm {
		reply.Success = false
	} else if args.Term == rf.CurrentTerm {

		rf.resetElectionTimer()

		if rf.currentState == LEADER{
			log.Fatal("two leaders same terms")
		}
		if rf.currentState == CANDIDATE {
			rf.currentState = FOLLOWER
			rf.receivedVotes = 0
		}

		rf.receiverAppendEntriesHandling(args, reply)
	} else {

		rf.increaseTermTo(args.Term)

		if rf.currentState == LEADER {
			rf.leaderToFollower()
		} else if rf.currentState == CANDIDATE {
			rf.candidateToFollower()
		}

		rf.receiverAppendEntriesHandling(args, reply)
	}

	reply.Term = rf.CurrentTerm
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {

	timeout := time.NewTimer(time.Duration(RPC_SENDING_TIMEOUT) * time.Millisecond)
	result := make(chan bool, 1)

	go func() {
		result<-rf.peers[server].Call("Raft.HandleAppendEntries", args, reply)
		return
	}()

	select {
	case <-timeout.C:
		return false
	case output := <-result:
		return output
	}
}

func (rf *Raft) receiverInstallSnapshotHandling(args InstallSnapshotArgs, reply *InstallSnapshotReply) {

	if args.LastIndexIncluded <= rf.LastIndexIncluded {
		return
	}

	previousLastIndexIncluded := rf.LastIndexIncluded
	
	followerHasMatchingLogTerm := false
	matchingIndex := 0
	for i:=0; i<len(rf.Log); i++ {
		if rf.Log[i].Term == args.LastTermIncluded && i+previousLastIndexIncluded+1 == args.LastIndexIncluded {
			followerHasMatchingLogTerm = true
			matchingIndex = i
			break
		}
	}

	// rf.LastIndexIncluded = args.LastIndexIncluded
	// rf.LastTermIncluded = args.LastTermIncluded

	// Then scan through the log to find a matching Term/Index entry, 
	// If found delete all entries upto and including, update snapshot on kvraft if lastApplied is less than LastIndexIncluded
	if followerHasMatchingLogTerm {

		// Ordering of updates?

		if rf.lastApplied < args.LastIndexIncluded {

			snapshotToSendToKV := ApplyMsg{UseSnapshot: true, Snapshot: args.SnapshotData, Term: args.LastTermIncluded, Index: args.LastIndexIncluded}
			rf.applyChannel <- snapshotToSendToKV

			rf.commitIndex = args.LastIndexIncluded
			rf.lastApplied = args.LastIndexIncluded
			newLog := make([]LogEntry, len(rf.Log) - matchingIndex - 1)
			copy(newLog, rf.Log[(matchingIndex+1):])
			rf.Log = newLog
		}
		// rf.commitIndex = args.LastIndexIncluded
		// rf.lastApplied = args.LastIndexIncluded
		// newLog := make([]LogEntry, len(rf.Log) - matchingIndex - 1)
		// copy(newLog, rf.Log[(matchingIndex+1):])
		// rf.Log = newLog
		// rf.persist()
	} else {
	// If not found:
		// Discard the entire log (just create a new slice)
		// Send a UseSnapshot=true ApplyMsg to applyChannel for the kvserver to update using the snapshot
		snapshotToSendToKV := ApplyMsg{UseSnapshot: true, Snapshot: args.SnapshotData, Term: args.LastTermIncluded, Index: args.LastIndexIncluded}
		rf.applyChannel <- snapshotToSendToKV

		rf.commitIndex = args.LastIndexIncluded
		rf.lastApplied = args.LastIndexIncluded
		rf.Log = make([]LogEntry, 0)
	}

	rf.LastIndexIncluded = args.LastIndexIncluded
	rf.LastTermIncluded = args.LastTermIncluded

	reply.Success = true
	reply.FirstIndexWithConflictingTerm = args.LastIndexIncluded + 1

	// LAB 4 new try?
 	// rf.persist()
	rf.persist()
	rf.persister.SaveSnapshot(args.SnapshotData)

}

func (rf *Raft) HandleInstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()

	reply.LeadershipTerm = args.Term

	if args.Term < rf.CurrentTerm {
		reply.Success = false
	} else if args.Term == rf.CurrentTerm {

		rf.resetElectionTimer()

		if rf.currentState == CANDIDATE {
			rf.currentState = FOLLOWER
			rf.receivedVotes = 0
		}

		rf.receiverInstallSnapshotHandling(args, reply)
	} else {

		rf.increaseTermTo(args.Term)

		if rf.currentState == LEADER {
			rf.leaderToFollower()
		} else if rf.currentState == CANDIDATE {
			rf.candidateToFollower()
		}

		rf.receiverInstallSnapshotHandling(args, reply)
	}

	reply.Term = rf.CurrentTerm
	return
}


func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {

	timeout := time.NewTimer(time.Duration(RPC_SENDING_TIMEOUT) * time.Millisecond)
	result := make(chan bool, 1)

	go func() {
		result<-rf.peers[server].Call("Raft.HandleInstallSnapshot", args, reply)
		return
	}()

	select {
	case <-timeout.C:
		return false
	case output := <-result:
		return output
	}
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()

	//Send back the electionTerm in requestVoteReply
	reply.ElectionTerm = args.Term

	if rf.CurrentTerm > args.Term {

    	reply.VoteGranted = false
    } else if rf.CurrentTerm == args.Term {
    	
    	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateID {

    		if rf.logsAreBehind(args.LastLogTerm, args.LastLogIndex) {

    			rf.VotedFor = args.CandidateID
	    		reply.VoteGranted = true
	    		rf.resetElectionTimer()
	    		rf.persist()
			} else {
				reply.VoteGranted = false
			}
    	} else {
    		reply.VoteGranted = false
    	}
    } else {
    	//If rf.current is less than args.Term
    	rf.increaseTermTo(args.Term)

    	if rf.currentState == LEADER {
    		rf.leaderToFollower()
    	} else if rf.currentState == CANDIDATE {
    		rf.candidateToFollower()
    	}

    	// We know votedFor is -1 b/c we increased
    	if rf.logsAreBehind(args.LastLogTerm, args.LastLogIndex) {
			rf.VotedFor = args.CandidateID
    		reply.VoteGranted = true
			rf.resetElectionTimer()
			rf.persist()
		} else {
			reply.VoteGranted = false
		}
    }

    reply.Term = rf.CurrentTerm //This will always be what we want because we increase
    return
}


func (rf *Raft) logsAreBehind(candidateTerm int, candidateIndex int) bool {

	if len(rf.Log)+rf.LastIndexIncluded+1 == 0 {
		return true
	}
	// Getting here means there have been log entries but current log might be 0 b/c of snapshotting so much check
	// If so just use the last included index and term in the snapshot
	ourLogTerm := 0
	ourHighestIndex := 0

	if len(rf.Log) == 0 {
		ourLogTerm = rf.LastTermIncluded
		ourHighestIndex = rf.LastIndexIncluded
	} else {
		// We have log elements so we can safely do this
		ourLogTerm = rf.Log[len(rf.Log)-1].Term 
		ourHighestIndex = (len(rf.Log)-1)+rf.LastIndexIncluded+1
	}

	if ourLogTerm < candidateTerm {
		return true
	} else if ourLogTerm == candidateTerm {
		if ourHighestIndex <= candidateIndex {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {

	timeout := time.NewTimer(time.Duration(RPC_SENDING_TIMEOUT) * time.Millisecond)
	result := make(chan bool, 1)

	go func() {
		result<-rf.peers[server].Call("Raft.RequestVote", args, reply)
		return
	}()

	select {
	case <-timeout.C:
		return false
	case output := <-result:
		return output
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. 
// if this server isn't the leader, returns false. 
//
// otherwise start the agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentState != LEADER {
		return -1, -1, false
	}

	newLog := LogEntry{Term: rf.CurrentTerm, Command: command}
	rf.Log = append(rf.Log, newLog)
	rf.persist()

	return len(rf.Log)+rf.LastIndexIncluded+1, rf.CurrentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	close(rf.shutdownChannel)
}

func (rf *Raft) Follower() {

	rf.mu.Lock()

	for rf.currentState == FOLLOWER {  // This check probably not necessary
		select{
		case <-rf.shutdownChannel:
			rf.mu.Unlock()
			return
		default:
			rf.mu.Unlock() //Every time we enter loop we should be locked
			rf.mu.Lock()

			select{

			//Case 1: Election Timer goes off
			case<-rf.electionTimer.C:

				//Convert necessary values to Candidate
				rf.followerToCandidate()

				// Start a candidate loop with new increased-by-1 term
				go rf.Candidate(rf.CurrentTerm)
				rf.mu.Unlock()
				return

			//Default we just do nothing
			default:
				//PASS
			}

			rf.mu.Unlock()
			time.Sleep(time.Duration(LOOP_SLEEP_AMOUNT) * time.Millisecond)
			rf.mu.Lock()
		}
	}

	rf.mu.Unlock()
	return //Once we change state, we just stop the method
}

func (rf *Raft) Candidate(electionTerm int) {

	// Start a go routine for each peer before we enter the loop, resending RPC's until we get a vote
		// Timer is taken care of with the campaigning for vote separate timer for each peer
		// so each peer can only vote one time 

	//Construct the args first
	rf.mu.Lock()
	
	thisLastLogIndex := -1
	thisLastLogTerm := -1

	if len(rf.Log)+rf.LastIndexIncluded+1 != 0 { // There have been entires in the past

		if len(rf.Log) == 0 { // Currently there are no entries because of snapshotting
			thisLastLogTerm = rf.LastTermIncluded
			thisLastLogIndex = rf.LastIndexIncluded
		} else {
			thisLastLogTerm = rf.Log[len(rf.Log)-1].Term
			thisLastLogIndex = (len(rf.Log)-1)+rf.LastIndexIncluded+1

		}
	}
	args := RequestVoteArgs{Term: rf.CurrentTerm, CandidateID: rf.me, LastLogIndex: thisLastLogIndex, LastLogTerm: thisLastLogTerm}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer != rf.me {

			go rf.campaignForVote(peer, electionTerm, args)
		}
	}

	rf.mu.Lock()

	// If we stop being a candidate or increase the term we just exit the for loop and return
	for rf.currentState == CANDIDATE && rf.CurrentTerm == electionTerm {

		rf.mu.Unlock() //Just like before we will always hold lock when we get here
		rf.mu.Lock()

		// Select between:
			// On the votes channel and every time we get a vote check for Quorum
				// When we get a vote, check to see if the electionTerm is the same,
				// only increment and check for quorum if it is from correct election term
					
			// Election Timer Channel: reset votes received counter and increase term

		select {
		case reply := <-rf.votesChannel:
			
			if rf.currentState == CANDIDATE && rf.CurrentTerm == reply.ElectionTerm {

				rf.receivedVotes++

				if 2 * rf.receivedVotes > len(rf.peers) {

					rf.candidateToLeader()
					go rf.Leader(rf.CurrentTerm)

					rf.mu.Unlock()
					return
				}
			}
		case <-rf.electionTimer.C:
			
			// Increase term, start a new candidate loop, reset votesFor,
			rf.candidateToCandidate()
			go rf.Candidate(rf.CurrentTerm) // Start an election in the next term

			rf.mu.Unlock()
			return
		default:
			//Do nothing, so lock is released wen we have nothing to do
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(LOOP_SLEEP_AMOUNT) * time.Millisecond)
		rf.mu.Lock()
	}

	// Should always be a follower in this state never get here if we increased term
	// If we increase term but still are candidate it means electionTimer went off so
		// should have just call the goroutine and returned from that loop
	rf.mu.Unlock()
	go rf.Follower()
	return
}

func (rf *Raft) Leader(leaderTerm int) {

	// Make the first thing send immediately
	rf.heartbeatTimer = time.NewTimer(time.Duration(1) * time.Millisecond)

	rf.mu.Lock()

	for rf.currentState == LEADER && rf.CurrentTerm == leaderTerm {

		select{
		case <-rf.shutdownChannel:
			rf.mu.Unlock()
			return
		default:
			rf.mu.Unlock() //Just like before we will always hold lock when we get here

			select {
			case<-rf.heartbeatTimer.C:

				rf.resetHeartbeatTimer()

				//for each of the peers call a goroutine to send the appropriate AppendEntries
				for p := range rf.peers {
					if p != rf.me {
						go rf.sendLeaderMessageToPeer(p)
					}
				}
			default:
				//Do nothing
			}

			time.Sleep(time.Duration(LOOP_SLEEP_AMOUNT) * time.Millisecond)
			rf.mu.Lock()
		}
	}

	// If we exit the loop because we no longer a leader then start being a follower again
	rf.mu.Unlock()
	go rf.Follower()
	return
}
 
func (rf *Raft) sendLeaderMessageToPeer(peer int) {

	// Lock
	// Creates the proper AppendEntires thing
	// Unlock

	// Send RPC

	// Lock
	// Process the reply
	// Unlock

	rf.mu.Lock()

	if rf.currentState != LEADER {
		rf.mu.Unlock()
		return
	}

	peerPrevLogIndex := -1
	peerPrevLogTerm := -1
	entriesToSend := make([]LogEntry, 0)

	// If the index we need to send to the follower is lower or equal to our rf.LastIndexIncluded,
	// it means we don't have it and we need to send a snapshot rpc instead
	if rf.nextIndex[peer] <= rf.LastIndexIncluded {
		// Very similar code to the AppendEntries sending/handling

		thisPeerNextIndex := rf.nextIndex[peer]
		// Create args and reply stucts
		args := InstallSnapshotArgs{Term: rf.CurrentTerm, LeaderID: rf.me, LastIndexIncluded: rf.LastIndexIncluded, LastTermIncluded: rf.LastTermIncluded, SnapshotData: rf.persister.ReadSnapshot()}
		reply := new(InstallSnapshotReply)
		
		rf.mu.Unlock()

		ok := false

		for i := 0; i < TIMES_TO_RETRY_RPC; i++ {

			reply = new(InstallSnapshotReply)
			ok = rf.sendInstallSnapshot(peer, args, reply)
			if ok {
				break
			}
		}

		if ok == false {
			return
		}

		rf.mu.Lock()

		if rf.currentState == LEADER && rf.CurrentTerm == reply.LeadershipTerm && rf.nextIndex[peer] == thisPeerNextIndex { // Make sure we don't double append

			if reply.Term > rf.CurrentTerm {

				rf.increaseTermTo(reply.Term)
				rf.leaderToFollower()
			} else {
				if reply.Success {

					// All steps in reciever implementation from figure 13 carried out		
					rf.nextIndex[peer] = rf.LastIndexIncluded + 1 // everything taken care of up to this point
					rf.matchIndex[peer] = rf.LastIndexIncluded // Snapshot takes care of this

					rf.tryIncreaseLeaderCommitIndex()
				} else {
					// Do nothing just retry next time we call sendLeaderMessageToPeer b/c follower didn't get it
				}			
			}
		}

		rf.mu.Unlock()
		return
	}

	if len(rf.Log)+rf.LastIndexIncluded+1 > 0 && rf.nextIndex[peer] >= 0 {
		
		peerPrevLogIndex = rf.nextIndex[peer]-1
		entriesToSend = append(entriesToSend, rf.Log[(rf.nextIndex[peer]-rf.LastIndexIncluded-1):]...)
	}

	// If the index of item we need to check is the last one we snapshotted assign Term using rf.LastTermIncluded don't index
	if peerPrevLogIndex == rf.LastIndexIncluded {
		peerPrevLogTerm = rf.LastTermIncluded
	} else if peerPrevLogIndex >= 0 {
		peerPrevLogTerm = rf.Log[peerPrevLogIndex-rf.LastIndexIncluded-1].Term
	}


	// If last log index >= nextIndex sent logs
	// If not then heartbeat (empty entries)
	if len(rf.Log)-1+rf.LastIndexIncluded+1 < rf.nextIndex[peer] { 
		//Makes sense because this is saying next index peer expects is something we don't yet hav
		entriesToSend = make([]LogEntry, 0)
	}

	thisPeerNextIndex := rf.nextIndex[peer]

	args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderID: rf.me, PrevLogIndex: peerPrevLogIndex, PrevLogTerm: peerPrevLogTerm, Entries: entriesToSend, LeaderCommit: rf.commitIndex}
	reply := new(AppendEntriesReply)


	rf.mu.Unlock()

	ok := false

	for i := 0; i < TIMES_TO_RETRY_RPC; i++ {

		reply = new(AppendEntriesReply)
		ok = rf.sendAppendEntries(peer, args, reply)
		if ok {
			break
		}
	}

	if ok == false {
		return
	}

	rf.mu.Lock()

	// Make sure we don't double append
	if rf.currentState == LEADER && rf.CurrentTerm == reply.LeadershipTerm && rf.nextIndex[peer] == thisPeerNextIndex { 

		if reply.Term > rf.CurrentTerm {

			rf.increaseTermTo(reply.Term)
			rf.leaderToFollower()
		} else {
			if reply.Success {

				// Means we had entries, sent them, got a success reply, 
				// so nextIndex and matchIndex change if we actually sent entries

				if len(entriesToSend) != 0 {
					rf.nextIndex[peer] = rf.nextIndex[peer] + len(entriesToSend)
					rf.matchIndex[peer] = rf.nextIndex[peer] - 1

					rf.tryIncreaseLeaderCommitIndex()
				}
			} else {
				if reply.FirstIndexWithConflictingTerm >= 0 {
					//Optimization to send less RPCs to get down to a log consensus
					rf.nextIndex[peer] = reply.FirstIndexWithConflictingTerm
				} else {
					rf.nextIndex[peer] = Max(0,rf.nextIndex[peer] - 1)
				}
			}			
		}
	}

	rf.mu.Unlock()
	return
}

func (rf *Raft) tryIncreaseLeaderCommitIndex() {

	// Just keep trying up to the end of the log
	newBestIndex := -1
	oldCommitIndex := rf.commitIndex

	if rf.commitIndex < rf.LastIndexIncluded {

		log.Error("Error: rf.commitIndex is less than rf.LastIndexIncluded when calling this")
		snapshotToSendToKV := ApplyMsg{UseSnapshot: true, Snapshot: rf.persister.ReadSnapshot()}
		rf.applyChannel <- snapshotToSendToKV

		rf.commitIndex = rf.LastIndexIncluded
		rf.lastApplied = rf.LastIndexIncluded
		return
	}

	for indexTry := oldCommitIndex+1; indexTry < len(rf.Log)+rf.LastIndexIncluded+1; indexTry++ {

		if rf.Log[indexTry-rf.LastIndexIncluded-1].Term == rf.CurrentTerm {

			count := 1
			for p := range rf.peers {
				if p != rf.me {
					if rf.matchIndex[p] >= indexTry {

						count++					
					}
				}
			}

			if 2*count > len(rf.peers) {

				newBestIndex = indexTry
			}
		}
		//else just move on to next one
	}

	if newBestIndex != -1 {

		rf.commitIndex = newBestIndex
	}
}

// Method that keeps trying to get votes from a peer
func (rf *Raft) campaignForVote(peer int, currentElectionTerm int, args RequestVoteArgs) {

	// Maybe make it initially go off immediately so it requests ASAP
	individualPeerVoteTimer := time.NewTimer(time.Duration(1) * time.Millisecond)

	rf.mu.Lock()

	// Loop while we are still a candidate and currentTerm is same
	for rf.currentState == CANDIDATE && rf.CurrentTerm == currentElectionTerm {

		rf.mu.Unlock()
		//Every time the timer goes off
			// Create the reply
			// Send the RPC (not as goroutine b/c we wait til it finish (times out after 50ms))
			// Check reply
				// if voteGranted is false
					//restart the timer

				// if voteGranted is true, 
					// Send the reply something to votesChannel then return b/c don't need to campaign with 
					// this voter anymore

		<-individualPeerVoteTimer.C
		reply := new(RequestVoteReply)

		ok := rf.sendRequestVote(peer, args, reply)

		if ok == false {
			individualPeerVoteTimer.Reset(time.Duration(VOTES_TIMEOUT) * time.Millisecond)
		}

		rf.mu.Lock()

		if rf.currentState == CANDIDATE && rf.CurrentTerm == currentElectionTerm && reply.ElectionTerm == rf.CurrentTerm {

			if reply.VoteGranted == true {

				go func() {
					rf.votesChannel <- reply // Indicate to the votes channel that were successful
					return
				}()

				rf.mu.Unlock()
				return // Can end the goroutine
			} else {

				if reply.Term > rf.CurrentTerm { // We encounter something with larger term

					rf.increaseTermTo(reply.Term)
					rf.candidateToFollower()
				}
				// Reset the individual timer so we can request again if we don't get the vote
				// because maybe it's because the server is down or something not b/c we failed
				individualPeerVoteTimer.Reset(time.Duration(VOTES_TIMEOUT) * time.Millisecond)
			}
		} else {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
		rf.mu.Lock()
	}

	rf.mu.Unlock()
	return
}

func (rf *Raft) TrimLogsViaSnapshot(snapshotIndex int) {

	rf.mu.Lock()

	if snapshotIndex <= rf.LastIndexIncluded {
		rf.mu.Unlock()
		return
	}

	previousLastIndexIncluded := rf.LastIndexIncluded
	rf.lastApplied = snapshotIndex

	log.WithFields(log.Fields{"PreviousLastIndexIncluded": previousLastIndexIncluded,
		"NewLastIndexIncluded": snapshotIndex,}).Warn("Trimming logs on raft")

	if (snapshotIndex-previousLastIndexIncluded-1 >= len(rf.Log) || snapshotIndex-previousLastIndexIncluded-1 < 0) {

		log.WithFields(log.Fields{
			"Index": snapshotIndex-previousLastIndexIncluded-1,
			"Log Length": len(rf.Log),}).Error("Snapshot messed up gg")

		rf.mu.Unlock()
		return
	}

	rf.LastIndexIncluded = snapshotIndex
	rf.LastTermIncluded = rf.Log[snapshotIndex-previousLastIndexIncluded-1].Term

	itemsLeftInLog := len(rf.Log) - (rf.LastIndexIncluded - previousLastIndexIncluded) //Current length minus the difference between new and old index included
	
	newLog := make([]LogEntry, itemsLeftInLog)
	if itemsLeftInLog != 0 {
		copy(newLog, rf.Log[(rf.LastIndexIncluded - previousLastIndexIncluded):])
	}
	rf.Log = newLog
	rf.persist()

	rf.mu.Unlock()
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.VotedFor = -1
	rf.applyChannel = applyCh
	rf.votesChannel = make(chan *RequestVoteReply)
	rf.shutdownChannel = make(chan int)
	
	rf.LastIndexIncluded = -1
	rf.LastTermIncluded = -1

	rf.commitIndex = -1 //should be -1 to start because thing is committed
	rf.lastApplied = -1
	rf.Log = make([]LogEntry, 0)

	// Instantiate the electionTimer
	rf.electionTimer = time.NewTimer(time.Duration(ELECTION_TIMEOUT_BASE + rand.Intn(ELECTION_TIMEOUT_VARIANCE)) * time.Millisecond)
	
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.LastIndexIncluded
	rf.lastApplied = rf.LastIndexIncluded

	//Start the first follower loop
	go rf.Follower()

	go func(){
		for {
			select{
			case <- rf.shutdownChannel:
				return
			default:
				rf.mu.Lock()

				// If commitIndex is less than lastIndexIncluded then bring it up to date
				// If it is equal, then we can be sure (I think) that lastApplied will equal commitIndex
				if rf.commitIndex < rf.LastIndexIncluded {
					rf.commitIndex = rf.LastIndexIncluded
					rf.lastApplied = rf.LastIndexIncluded
					rf.mu.Unlock()
					continue
				} else if rf.commitIndex == rf.LastIndexIncluded  {
					rf.mu.Unlock()
					time.Sleep(time.Duration(20) * time.Millisecond)
					continue
				}

				// Else the commitIndex is greater than rf.LastIndexIncluded
				if rf.commitIndex > rf.lastApplied {

					// if rf.lastApplied < rf.LastIndexIncluded {
					// 	//Actually I don't think this should happen ever
					// 	log.Fatal("lastApplied is lower than LastIndexIncluded somehow")						
	
					commitFromHere := rf.lastApplied+1 
					correspondingLastIndexIncluded := rf.LastIndexIncluded

					indexToSend := commitFromHere+1
					termToSend := rf.Log[commitFromHere-correspondingLastIndexIncluded-1].Term
					commandToSend := rf.Log[commitFromHere-correspondingLastIndexIncluded-1].Command

					rf.lastApplied++
					rf.mu.Unlock()

					rf.applyChannel <- ApplyMsg{Index: indexToSend, Term: termToSend, Command: commandToSend}

					continue	
				} else {
					rf.mu.Unlock()	
					time.Sleep(time.Duration(20) * time.Millisecond)
				}
			}
		}
	}()

	return rf
}
