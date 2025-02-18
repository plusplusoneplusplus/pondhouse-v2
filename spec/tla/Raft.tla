------------------------------- MODULE Raft -------------------------------
EXTENDS Integers, Sequences, TLC, FiniteSets, Utilities

CONSTANTS
    Servers,        \* The set of servers in the cluster
    ClientRequests, \* The set of possible client requests
    Nil,            \* Represents empty value
    MaxTerm         \* The maximum term in the system

\* Per-server state
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

\* Global State
VARIABLE messages   \* see TypeInvariant

\* History
VARIABLE stateHistory \* [server \in Servers |-> [0..MaxTerm -> {"Follower", "Candidate", "Leader"}]]

vars == <<state, currentTerm, votedFor, logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, messages, stateHistory>>

(*****************************************************************************)
(* Type invariants                                                           *)
(*****************************************************************************)
TypeInvariant ==
    /\ state \in [Servers -> {"Follower", "Candidate", "Leader"}]
    /\ currentTerm \in [Servers -> Nat]
    /\ votedFor \in [Servers -> (Servers \cup {Nil})]
    /\ logs \in [Servers -> Seq([term |-> Nat, cmd |-> (ClientRequests \cup {Nil})])]
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
           /\ m.entries \in Seq([term |-> Nat, cmd |-> (ClientRequests \cup {Nil})])
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
    \* Test Assert when a leader is elected
    /\ \A s \in Servers : state[s] /= "Leader"
    \* Test Assert when a response from vote request is generated
    \* /\ \A m \in DOMAIN messages : m.type /= "RequestVoteResponse"
    \* Test Assert when a server has not granted any votes
    \* /\ \A s \in Servers : Cardinality(votesGranted[s]) = 0

(*****************************************************************************)
(* Initialization                                                            *)
(*****************************************************************************)

InitPersistentState ==
    /\ currentTerm = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ logs = [s \in Servers |-> <<>>]

InitVolatileState ==
    \* Volatile State
    /\ state = [s \in Servers |-> "Follower"]
    /\ commitIndex = [s \in Servers |-> 0]
    /\ lastApplied = [s \in Servers |-> 0]
    \* Volatile Leader State
    /\ nextIndex = [s \in Servers |-> [s2 \in Servers |-> 1]]
    /\ matchIndex = [s \in Servers |-> [s2 \in Servers |-> 0]]
    \* Volatile Candidate State
    /\ votesGranted = [s \in Servers |-> {}]

InitHistory ==
    /\ stateHistory = [s \in Servers |-> <<>>]

Init ==
    /\ InitPersistentState
    /\ InitVolatileState
    /\ InitHistory
    /\ messages = [m \in {} |-> 0]


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

\* Helper method to update the state and state history
UpdateStateWithHistory(s, term, newState) ==
    IF state[s] /= newState THEN
        /\ stateHistory' = [stateHistory EXCEPT ![s] = stateHistory[s] @@ [state |-> newState, term |-> term]]
        /\ state' = [state EXCEPT ![s] = newState]
    ELSE
        UNCHANGED <<state, stateHistory>>

(*****************************************************************************)
(* Actions                                                                   *)
(*****************************************************************************)

\* Restart a server
\* Persistent States are not reset while volatile states are reset
Restart(s) ==
    /\ UpdateStateWithHistory(s, -1, "Follower")
    /\ commitIndex' = [commitIndex EXCEPT ![s] = 0]
    /\ lastApplied' = [lastApplied EXCEPT ![s] = 0]
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [s2 \in Servers |-> Len(logs[s]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [s2 \in Servers |-> 0]]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ UNCHANGED <<currentTerm, votedFor, logs, messages, stateHistory>>

\* Start a new election with rules for candidate
\* 1. Increment term
\* 2. Vote for self
\* 3. Reset vote count
BecomeCandidate(s) ==
    /\ state[s] /= "Leader"
    /\ currentTerm[s] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ votesGranted' = [votesGranted EXCEPT ![s] = {}]
    /\ UpdateStateWithHistory(s, currentTerm[s], "Candidate")
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, messages>>

\* Send RequestVote RPCs from s1 to s2
SendRequestVote(s1, s2) ==
    /\ s1 /= s2
    /\ state[s1] = "Candidate"
    /\ Send([
        type |-> "RequestVote",
        term |-> currentTerm[s1],
        candidateId |-> s1,
        lastLogIndex |-> Len(logs[s1]),
        lastLogTerm |-> IF Len(logs[s1]) > 0 THEN logs[s1][Len(logs[s1])].term ELSE 0,
        from |-> s1,
        to |-> s2])
    /\ UNCHANGED <<currentTerm, votedFor, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted, stateHistory>>

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
           /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, votesGranted>>
       ELSE
           /\ Reply([
               type |-> "RequestVoteResponse",
               term |-> currentTerm[r],
               voteGranted |-> FALSE,
               from |-> r,
               to |-> s], m)
           /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, currentTerm, votedFor, state, votesGranted, stateHistory>>

ProcessRequestVoteResponse(r, s, m) ==
    /\ state[r] = "Candidate"
    /\ m.term = currentTerm[r]
    /\ \/ /\ m.voteGranted = TRUE
          /\ votesGranted' = [votesGranted EXCEPT ![r] = votesGranted[r] \cup {s}]
       \/ /\ m.voteGranted = FALSE
          /\ UNCHANGED <<votesGranted>>
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votedFor, stateHistory>>

UpdateFromMessage(r, s, m) ==
    /\ m.term > currentTerm[r]
    /\ currentTerm' = [currentTerm EXCEPT ![r] = m.term]
    /\ UpdateStateWithHistory(r, m.term, "Follower")
    /\ votedFor' = [votedFor EXCEPT ![r] = Nil]
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex, messages, votesGranted>>

DropStaleMessage(m) ==
    /\ m.term < currentTerm[m.to]
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, logs, commitIndex, lastApplied, nextIndex, matchIndex, votedFor, votesGranted, stateHistory>>

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

BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ votesGranted[s] \in Quorum
    /\ UpdateStateWithHistory(s, currentTerm[s], "Leader")
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [f \in Servers |-> Len(logs[s]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [f \in Servers |-> 0]]
    /\ UNCHANGED <<currentTerm, votedFor, logs, commitIndex, lastApplied, votesGranted, messages>>

\* The following are stub actions for completeness.
ReceiveVoteRequest(s) ==
    /\ UNCHANGED vars

ReceiveVoteResponse(s) ==
    /\ UNCHANGED vars

AdvanceCommitIndex(s) ==
    /\ UNCHANGED vars

ApplyCommittedEntries(s) ==
    /\ UNCHANGED vars

Next ==
    \/ \E s \in Servers : Restart(s)
    \/ \E s \in Servers : BecomeCandidate(s)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E s1, s2 \in Servers : SendRequestVote(s1, s2)
    \/ \E m \in DOMAIN messages : ProcessMessage(m)
    \* \/ \E s \in Servers : LeaderAppendEntries(s)
    \* \/ \E s \in Servers : ReceiveAppendEntriesResponse(s)
    \* \/ \E s \in Servers : ReceiveVoteRequest(s)
    \* \/ \E s \in Servers : ReceiveVoteResponse(s)
    \* \/ \E s \in Servers : AdvanceCommitIndex(s)
    \* \/ \E s \in Servers : ApplyCommittedEntries(s)

\* Termination ==
\*     <>(\E s \in Servers : state[s] = "Leader")  \* Eventually elect a leader

\* Liveness ==
\*     \A s \in Servers : WF_vars(LeaderAppendEntries(s)) /\ WF_vars(BecomeCandidate(s))

Spec == Init /\ [][Next]_vars

=============================================================================
