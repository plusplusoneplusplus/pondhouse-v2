------------------------------- MODULE Raft -------------------------------
\*
\* This module is a specification of the Raft consensus protocol.
\* A lot of the code is taken from the paper "In Search of an Understandable Consensus Algorithm"
\* and the existing tla+ from https://github.com/ongardie/raft.tla
\*

EXTENDS Integers, Sequences, TLC, FiniteSets, Utilities

CONSTANTS
    Servers,        \* The set of servers in the cluster
    ClientRequests, \* The set of possible client requests
    Nil,            \* Represents empty value
    MaxTerm         \* The maximum term in the system

\*
\* Per-server state
\*
VARIABLES
    \* Persisted State for each server, update to storage before responding to RPC
    currentTerm,    \* [server \in Servers |-> Nat]
    votedFor,       \* [server \in Servers |-> Servers \cup {Nil}]
    logs,           \* [server \in Servers |-> Seq([term |-> Nat, cmd |-> ClientRequests \cup {Nil}])]
    \* Volatile State for each server
    state,          \* [server \in Servers |-> {"Follower", "Candidate", "Leader"}]
    commitIndex,    \* [server \in Servers |-> Nat]
    lastApplied,    \* [server \in Servers |-> Nat]
    \* Volatile Leader State, reinitialized on election
    nextIndex,      \* [server \in Servers |-> [Servers -> Nat]]
    matchIndex,     \* [server \in Servers |-> [Servers -> Nat]]
    \* Volatile Candidate State
    votesGranted    \* [server \in Servers |-> SUBSET(Servers)]

\*
\* Global State
\*
VARIABLE messages   \* see TypeInvariant

\*
\* History, kept for debugging purposes
\*
VARIABLE stateHistory, \* [server \in Servers |-> [0..MaxTerm -> {"Follower", "Candidate", "Leader"}]]
         restartHistory \* [server \in Servers |-> Nat]

vars == <<state, currentTerm, votedFor, logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, messages, stateHistory, restartHistory>>

(*****************************************************************************)
(* Helper functions                                                          *)
(*****************************************************************************)

LastLogTerm(s) == IF Len(logs[s]) > 0 THEN logs[s][Len(logs[s])].term ELSE 0

\* The set of all quorums. This just calculates simple majorities, but the only
\* important property is that every quorum overlaps with every other.
Quorum == {i \in SUBSET(Servers) : Cardinality(i) * 2 > Cardinality(Servers)}

\* Add a message to the bag of messages.
Send(m) == messages' = WithMessage(m, messages)

\* Remove a message from the bag of messages. Used when a server is done
\* processing a message.
Discard(m) == messages' = WithoutMessage(m, messages)

\* Combination of Send and Discard
Reply(response, request) ==
    messages' = WithoutMessage(request, WithMessage(response, messages))

ValidMessage(msgs) == { m \in DOMAIN messages : msgs[m] > 0 }

InvalidMessage(msgs) == { m \in DOMAIN messages : msgs[m] = 0 }

GetMessageByType(msgs, t) == { m \in ValidMessage(msgs) : m.type = t }

\* Helper method to update the state and state history
UpdateStateWithHistory(s, term, newState) ==
    IF state[s] /= newState THEN
        /\ stateHistory' = [stateHistory EXCEPT ![s] = Append(
            stateHistory[s], [state |-> newState, term |-> term])]
        /\ state' = [state EXCEPT ![s] = newState]
    ELSE
        UNCHANGED <<state, stateHistory>>

(*****************************************************************************)
(* Type invariants                                                           *)
(*****************************************************************************)
TypeInvariant ==
    /\ state \in [Servers -> {"Follower", "Candidate", "Leader"}]
    /\ currentTerm \in [Servers -> Nat]
    /\ votedFor \in [Servers -> (Servers \cup {Nil})]
    /\ \A s \in Servers :
        \A i \in 1..Len(logs[s]) :
            /\ logs[s][i].term \in Nat
            /\ logs[s][i].cmd \in {Nil} \cup ClientRequests
    /\ commitIndex \in [Servers -> Nat]
    /\ lastApplied \in [Servers -> Nat]
    /\ nextIndex \in [Servers -> [Servers -> Nat]]
    /\ matchIndex \in [Servers -> [Servers -> Nat]]
    /\ \A m \in DOMAIN messages :
        \/ /\ m.type = "AppendEntries"
           /\ m.term \in Nat
           /\ m.leaderId \in Servers
           /\ m.prevLogIndex \in Nat
           /\ m.prevLogTerm \in Nat
           /\ \A i \in 1..Len(m.entries) :
               /\ m.entries[i].term \in Nat
               /\ m.entries[i].cmd \in {Nil} \cup ClientRequests
           /\ m.leaderCommit \in Nat
           /\ m.from \in Servers
           /\ m.to \in Servers
        \/ /\ m.type = "AppendEntriesResponse"
           /\ m.term \in Nat
           /\ m.success \in BOOLEAN
           /\ m.from \in Servers
           /\ m.to \in Servers
        \/ /\ m.type = "RequestVote"
           /\ m.term \in Nat
           /\ m.candidateId \in Servers
           /\ m.lastLogIndex \in Nat
           /\ m.lastLogTerm \in Nat
           /\ m.from \in Servers
           /\ m.to \in Servers
        \/ /\ m.type = "RequestVoteResponse"
           /\ m.term \in Nat
           /\ m.voteGranted \in BOOLEAN
           /\ m.from \in Servers
           /\ m.to \in Servers
    /\ votesGranted \in [Servers -> SUBSET(Servers)]
    /\ restartHistory \in [Servers -> Nat]

Safety ==
    /\ \* Election Safety: At most one leader per term
       \A t \in 0..MaxTerm : \A s1, s2 \in Servers :
           (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = t)
           => (s1 = s2)
    /\ \* Leader Append-Only: Leaders only append new entries
       \A s \in Servers : \A i \in DOMAIN logs[s] :
           logs[s][i].term <= currentTerm[s]
    /\ \* State Machine Safety
       \A s1, s2 \in Servers : \A i \in 1..Min({lastApplied[s1], lastApplied[s2]}) :
           logs[s1][i].term = logs[s2][i].term => logs[s1][i].cmd = logs[s2][i].cmd
    /\ \* Commit Index Validity
       \A s \in Servers : commitIndex[s] <= Len(logs[s])
    /\ \* Last Applied Validity
       \A s \in Servers : lastApplied[s] <= commitIndex[s]
    /\ \* Voter Safety
       \A s \in Servers : votedFor[s] \in (Servers \cup {Nil})
    /\ \* Leader Log Completeness
       \A s \in Servers : \A f \in Servers :
           state[s] = "Leader" =>
               SubSeq(logs[f], 1, matchIndex[s][f]) = SubSeq(logs[s], 1, matchIndex[s][f])
    /\ \* Next Index Consistency
       \A s \in Servers : \A f \in Servers :
           nextIndex[s][f] <= Len(logs[s]) + 1

TestInvariant == /\ TRUE
    \*
    \*
    \* Test Assert when a response from vote request is generated
    \* /\ \A m \in DOMAIN messages : m.type /= "RequestVoteResponse"
    \*
    /\ \A s \in Servers :
        \*
        \* Test Assert when a leader is elected
        \*
        \* /\ state[s] /= "Leader"
        \*
        \* Test Assert when a server has not granted any votes
        \* /\ Cardinality(votesGranted[s]) = 0
        \*
        \* Test Assert that a server cannot be leader twice
        \*
        /\ LET
            leaderStates == {i \in 1..Len(stateHistory[s]) : stateHistory[s][i].state = "Leader"}
           IN
            Cardinality(leaderStates) <= 1
        \*
        \* Test Assert that follower has log entry
        \*
        \* /\ (state[s] = "Follower" /\ Len(stateHistory[s]) = 0) => Len(logs[s]) = 0

(*****************************************************************************)
(* Initialization                                                            *)
(*****************************************************************************)

InitPersistentState ==
    /\ currentTerm = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ logs = [s \in Servers |-> <<>>]

InitServerState ==
    \* /\ state = [s \in Servers |-> "Follower"]
    /\ state = [s \in Servers |-> IF s = CHOOSE srv \in Servers : TRUE THEN "Leader" ELSE "Follower"]

InitVolatileState ==
    \* Volatile State
    /\ InitServerState
    /\ commitIndex = [s \in Servers |-> 0]
    /\ lastApplied = [s \in Servers |-> 0]
    \* Volatile Leader State
    /\ nextIndex = [s \in Servers |-> [s2 \in Servers |-> 1]]
    /\ matchIndex = [s \in Servers |-> [s2 \in Servers |-> 0]]
    \* Volatile Candidate State
    /\ votesGranted = [s \in Servers |-> {}]

InitHistory ==
    /\ stateHistory = [s \in Servers |-> <<>>]
    /\ restartHistory = [s \in Servers |-> 0]

Init ==
    /\ InitPersistentState
    /\ InitVolatileState
    /\ InitHistory
    /\ messages = [m \in {} |-> 0]

(*****************************************************************************)
(* Actions                                                                   *)
(*****************************************************************************)

\* Restart a server
\* Persistent States are not reset while volatile states are reset
Restart(s) ==
    /\ restartHistory[s] < 1
    /\ restartHistory' = [restartHistory EXCEPT ![s] = restartHistory[s] + 1]
    /\ UpdateStateWithHistory(s, -1, "Follower")
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ lastApplied' = [lastApplied EXCEPT ![s] = 0]
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [s2 \in Servers |-> Len(logs[s]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [s2 \in Servers |-> 0]]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ UNCHANGED <<currentTerm, votedFor, logs, messages>>

\* Start a new election with rules for candidate
\* 1. Increment term
\* 2. Vote for self
\* 3. Reset vote count
BecomeCandidate(s) ==
    /\ state[s] = "Follower"
    /\ currentTerm[s] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ UpdateStateWithHistory(s, currentTerm[s], "Candidate")
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, messages, restartHistory>>

\* Send RequestVote RPCs from s1 to s2
SendRequestVote(s1, s2) ==
    /\ s1 /= s2
    /\ state[s1] = "Candidate"
    \* Reduce the state space by not sending RequestVote if a message already exists
    /\ ~\E m \in DOMAIN messages :
        /\ m.type = "RequestVote"
        /\ m.term = currentTerm[s1]
        /\ m.from = s1
        /\ m.to = s2
    /\ Send([
        type |-> "RequestVote",
        term |-> currentTerm[s1],
        candidateId |-> s1,
        lastLogIndex |-> Len(logs[s1]),
        lastLogTerm |-> IF Len(logs[s1]) > 0 THEN logs[s1][Len(logs[s1])].term ELSE 0,
        from |-> s1,
        to |-> s2])
    /\ UNCHANGED <<currentTerm, votedFor, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, stateHistory, restartHistory>>

ProcessRequestVote(r, s, m) ==
    /\ m.term = currentTerm[r]
    \* RequestVoteRPC, Receiver implementation condition 1 and 2
    /\ IF /\ votedFor[r] \in {Nil, m.candidateId}
          /\ \/ m.lastLogTerm > LastLogTerm(r)
             \/ /\ m.lastLogIndex >= Len(logs[r])
                /\ m.lastLogTerm = LastLogTerm(r)
       THEN
           /\ currentTerm' = [currentTerm EXCEPT ![r] = m.term]
           /\ votedFor' = [votedFor EXCEPT ![r] = m.candidateId]
           /\ UpdateStateWithHistory(r, currentTerm[r], "Follower")
           /\ Reply([
               type |-> "RequestVoteResponse",
               term |-> currentTerm[r],
               voteGranted |-> TRUE,
               from |-> r,
               to |-> s], m)
           /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, restartHistory>>
       ELSE
           /\ Reply([
               type |-> "RequestVoteResponse",
               term |-> currentTerm[r],
               voteGranted |-> FALSE,
               from |-> r,
               to |-> s], m)
           /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, currentTerm, votedFor, state, votesGranted, stateHistory, restartHistory>>

ProcessRequestVoteResponse(r, s, m) ==
    /\ state[r] = "Candidate"
    /\ m.term = currentTerm[r]
    /\ \/ /\ m.voteGranted = TRUE
          /\ votesGranted' = [votesGranted EXCEPT ![r] = votesGranted[r] \cup {s}]
       \/ /\ m.voteGranted = FALSE
          /\ UNCHANGED <<votesGranted>>
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votedFor, stateHistory, restartHistory>>

ProcessAppendEntriesReplyFalse(r, s, m, logOk) ==
    /\ \/ m.term < currentTerm[r]
       \/ /\ m.term = currentTerm[r]
          /\ logOk = FALSE
    /\ Reply([
        type |-> "AppendEntriesResponse",
        term |-> currentTerm[r],
        success |-> FALSE,
        matchIndex |-> 0,
        from |-> r,
        to |-> s], m)
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, restartHistory, state, stateHistory, votedFor, currentTerm>>

ProcessAppendEntries(r, s, m) ==
    LET logOk == \/ m.prevLogIndex = 0
                 \/ /\ m.prevLogIndex > 0
                    /\ m.prevLogIndex <= Len(logs[r])
                    /\ m.prevLogTerm = logs[r][m.prevLogIndex].term
    IN
        /\ m.term <= currentTerm[r]
        /\ \/ ProcessAppendEntriesReplyFalse(r, s, m, logOk)
           \/ /\ m.term = currentTerm[r]
              /\ \/ /\ state[r] = "Candidate"
                    /\ state' = [state EXCEPT ![r] = "Follower"]
                    /\ UNCHANGED <<currentTerm, votedFor, logs, commitIndex, lastApplied, nextIndex, matchIndex, messages, votesGranted, stateHistory, restartHistory>>
                 \/ /\ state[r] = "Follower"
                    /\ logOk = TRUE
                    \* /\ PrintT("R: " \o ToString(r) \o " Msg: " \o ToString(m) \o " " \o ToString(logs[r]))
                    /\ LET
                        index == m.prevLogIndex + 1
                       IN
                        \/ \* already have the valid entry.
                           \* This path is guaranteed to be hit when leader sends the message twice
                           \* and the follower has processed the first message which advances the log
                           \* entry.
                           /\ \/ m.entries = << >>
                              \/ /\ Len(logs[r]) >= index
                                 /\ logs[r][index].term = m.entries[1].term
                           /\ commitIndex' = [commitIndex EXCEPT ![r] = Min({Len(logs[r]), m.leaderCommit})]
                           /\ Reply([
                               type |-> "AppendEntriesResponse",
                               term |-> currentTerm[r],
                               success |-> TRUE,
                               matchIndex |-> m.prevLogIndex + Len(m.entries), \* Move to the next entry
                               from |-> r,
                               to |-> s], m)
                           /\ UNCHANGED <<currentTerm, votedFor, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, state, stateHistory, restartHistory>>
                        \/ \* conflict, remove existing entry by one.
                           /\ m.entries /= << >>
                           /\ Len(logs[r]) >= index
                           /\ logs[r][index].term /= m.entries[1].term
                           /\ LET new == [idx \in 1..(Len(logs[r]) - 1) |-> logs[r][idx]]
                              IN logs' = [logs EXCEPT ![r] = new]
                           /\ UNCHANGED <<currentTerm, votedFor, commitIndex, lastApplied, nextIndex, matchIndex, messages, votesGranted, state, stateHistory, restartHistory>>
                        \/ \* append the log entry and leave without sending the response, since
                           \* the leader will send the same message again.
                           /\ m.entries /= << >>
                           /\ Len(logs[r]) = m.prevLogIndex
                           /\ logs' = [logs EXCEPT ![r] = Append(logs[r], m.entries[1])]
                           /\ UNCHANGED <<currentTerm, votedFor, commitIndex, lastApplied, nextIndex, matchIndex, messages, votesGranted, state, stateHistory, restartHistory>>

ProcessAppendEntriesResponse(r, s, m) ==
    /\ m.term = currentTerm[r]
    /\ \/ /\ m.success = TRUE
          /\ nextIndex' = [nextIndex EXCEPT ![r][s] = m.matchIndex + 1]
          /\ matchIndex' = [matchIndex EXCEPT ![r][s] = m.matchIndex]
       \/ /\ m.success = FALSE
          \* decrease nextIndex if append entries failed
          /\ nextIndex' = [nextIndex EXCEPT ![r][s] = Max({nextIndex[r][s] - 1, 1})]
          /\ UNCHANGED <<matchIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<logs, commitIndex, lastApplied, votesGranted, restartHistory, state, stateHistory, votedFor, currentTerm>>

UpdateFromMessage(r, s, m) ==
    /\ m.term > currentTerm[r]
    /\ currentTerm' = [currentTerm EXCEPT ![r] = m.term]
    /\ UpdateStateWithHistory(r, m.term, "Follower")
    /\ votedFor' = [votedFor EXCEPT ![r] = Nil]
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, messages, votesGranted, restartHistory>>

DropStaleMessage(m) ==
    /\ m.term < currentTerm[m.to]
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votedFor, votesGranted, stateHistory, restartHistory>>

ProcessMessage(m) ==
    LET r == m.to
        s == m.from
    IN
        \/ UpdateFromMessage(r, s, m)
        \/ /\ m.type = "RequestVote"
           /\ ProcessRequestVote(r, s, m)
        \/ /\ m.type = "RequestVoteResponse"
           /\ \/ DropStaleMessage(m)
              \/ ProcessRequestVoteResponse(r, s, m)
        \/ /\ m.type = "AppendEntries"
           /\ ProcessAppendEntries(r, s, m)
        \/ /\ m.type = "AppendEntriesResponse"
           /\ \/ DropStaleMessage(m)
              \/ ProcessAppendEntriesResponse(r, s, m)

BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ votesGranted[s] \in Quorum
    /\ UpdateStateWithHistory(s, currentTerm[s], "Leader")
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [f \in Servers |-> Len(logs[s]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [f \in Servers |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, logs, commitIndex, lastApplied, votesGranted, messages, restartHistory>>

ClientRequest(s, v) ==
    /\ state[s] = "Leader"
    /\ logs' = [logs EXCEPT ![s] = Append(logs[s], [term |-> currentTerm[s], cmd |-> v])]
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, lastApplied, nextIndex, matchIndex, messages, stateHistory, restartHistory, votesGranted, state>>

\* l = leader, f = follower
\* leader will send
AppendEntries(l, f) ==
    /\ l /= f
    /\ state[l] = "Leader"
    /\ Len(logs[l]) >= nextIndex[l][f]
    /\ LET
        prevLogIndex == nextIndex[l][f] - 1
        prevLogTerm == IF prevLogIndex > 0 THEN logs[l][prevLogIndex].term ELSE 0
        \* take a single entry, in real world, it's a batch of entries
        lastEntryIndex == Min({Len(logs[l]), nextIndex[l][f]})
        leaderCommit == commitIndex[l]
        entries == SubSeq(logs[l], nextIndex[l][f], lastEntryIndex)
       IN
        Send([
            type |-> "AppendEntries",
            term |-> currentTerm[l],
            leaderId |-> l,
            prevLogIndex |-> prevLogIndex,
            prevLogTerm |-> prevLogTerm,
            entries |-> entries,
            leaderCommit |-> leaderCommit,
            from |-> l,
            to |-> f])
    /\ UNCHANGED <<currentTerm, votedFor, logs, commitIndex, lastApplied, nextIndex, matchIndex, stateHistory, restartHistory, state, votesGranted>>

Next ==
    \/ \E s \in Servers : Restart(s)
    \/ \E s \in Servers : BecomeCandidate(s)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E s1, s2 \in Servers : SendRequestVote(s1, s2)
    \/ \E m \in ValidMessage(messages) : ProcessMessage(m)
    \/ \E s \in Servers, v \in ClientRequests : ClientRequest(s, v)
    \/ \E s1, s2 \in Servers : AppendEntries(s1, s2)

Spec == Init /\ [][Next]_vars

=============================================================================
