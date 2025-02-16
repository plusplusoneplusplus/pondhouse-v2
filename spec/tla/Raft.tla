------------------------------- MODULE Raft -------------------------------
EXTENDS Integers, Sequences, TLC, FiniteSets, Utilities

CONSTANTS
    Servers,        \* The set of servers in the cluster
    ClientRequests, \* The set of possible client requests
    Nil,            \* Represents empty value
    MaxTerm         \* The maximum term in the system

VARIABLES
    state,          \* [server \in Servers |-> {"Follower", "Candidate", "Leader"}]
    currentTerm,    \* [server \in Servers |-> Nat]
    votedFor,       \* [server \in Servers |-> Servers \cup {Nil}]
    logs,           \* [server \in Servers |-> Seq([term |-> Nat, cmd |-> ClientRequests \cup {Nil}])]
    commitIndex,    \* [server \in Servers |-> Nat]
    lastApplied,    \* [server \in Servers |-> Nat]
    nextIndex,      \* [server \in Servers |-> [Servers -> Nat]]
    matchIndex      \* [server \in Servers |-> [Servers -> Nat]]

vars == <<state, currentTerm, votedFor, logs, commitIndex, lastApplied, nextIndex, matchIndex>>

TypeInvariant ==
    /\ state \in [Servers -> {"Follower", "Candidate", "Leader"}]
    /\ currentTerm \in [Servers -> Nat]
    /\ votedFor \in [Servers -> (Servers \cup {Nil})]
    /\ logs \in [Servers -> Seq([term |-> Nat, cmd |-> (ClientRequests \cup {Nil})])]
    /\ commitIndex \in [Servers -> Nat]
    /\ lastApplied \in [Servers -> Nat]
    /\ nextIndex \in [Servers -> [Servers -> Nat]]
    /\ matchIndex \in [Servers -> [Servers -> Nat]]

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

Init ==
    /\ state = [s \in Servers |-> "Follower"]
    /\ currentTerm = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ logs = [s \in Servers |-> <<>>]
    /\ commitIndex = [s \in Servers |-> 0]
    /\ lastApplied = [s \in Servers |-> 0]
    /\ nextIndex = [s \in Servers |-> [s2 \in Servers |-> 1]]
    /\ matchIndex = [s \in Servers |-> [s2 \in Servers |-> 0]]

BecomeCandidate(s) ==
    /\ state[s] /= "Leader"
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    /\ UNCHANGED <<logs, commitIndex, lastApplied, nextIndex, matchIndex>>

LeaderAppendEntries(s) ==
    /\ state[s] = "Leader"
    /\ \E entry \in ClientRequests :
         /\ logs' = [logs EXCEPT ![s] = Append(logs[s], [term |-> currentTerm[s], cmd |-> entry])]
         /\ nextIndex' = [nextIndex EXCEPT ![s] = [f \in Servers |-> IF f = s 
                                                                    THEN Len(logs'[s]) + 1
                                                                    ELSE nextIndex[s][f]]]
         /\ matchIndex' = [matchIndex EXCEPT ![s] = [f \in Servers |-> IF f = s 
                                                                      THEN Len(logs'[s])
                                                                      ELSE matchIndex[s][f]]]
         /\ UNCHANGED <<currentTerm, votedFor, state, commitIndex, lastApplied>>

ReceiveAppendEntriesResponse(s) ==
    /\ state[s] = "Leader"  \* Only leaders should process responses
    /\ \E src \in Servers \ {s} :
         /\ LET prevLogIndex == nextIndex[s][src] - 1
                prevLogTerm == IF prevLogIndex > 0 THEN logs[s][prevLogIndex].term ELSE 0
                entries == SubSeq(logs[s], nextIndex[s][src], Len(logs[s]))
            IN
                \/ \* Case 1: Log consistency check fails
                   /\ (\/ prevLogIndex > Len(logs[src])  \* Log is too short
                      \/ (prevLogIndex > 0 /\ logs[src][prevLogIndex].term /= prevLogTerm))  \* Term mismatch
                   /\ nextIndex' = [nextIndex EXCEPT ![s][src] = Max({1, nextIndex[s][src] - 1})]
                   /\ UNCHANGED <<currentTerm, votedFor, state, logs, commitIndex, lastApplied, matchIndex>>
                \/ \* Case 2: Log consistency check succeeds
                   /\ prevLogIndex <= Len(logs[src])
                   /\ (prevLogIndex = 0 \/ logs[src][prevLogIndex].term = prevLogTerm)
                   /\ logs' = [logs EXCEPT ![src] = SubSeq(logs[src], 1, prevLogIndex) \o entries]
                   /\ matchIndex' = [matchIndex EXCEPT ![s][src] = prevLogIndex + Len(entries)]
                   /\ nextIndex' = [nextIndex EXCEPT ![s][src] = matchIndex'[s][src] + 1]
                   /\ UNCHANGED <<currentTerm, votedFor, state, commitIndex, lastApplied>>

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
    \/ \E s \in Servers : BecomeCandidate(s)
    \/ \E s \in Servers : LeaderAppendEntries(s)
    \/ \E s \in Servers : ReceiveAppendEntriesResponse(s)
    \/ \E s \in Servers : ReceiveVoteRequest(s)
    \/ \E s \in Servers : ReceiveVoteResponse(s)
    \/ \E s \in Servers : AdvanceCommitIndex(s)
    \/ \E s \in Servers : ApplyCommittedEntries(s)

Termination ==
    <>(\E s \in Servers : state[s] = "Leader")  \* Eventually elect a leader

Liveness ==
    \A s \in Servers : WF_vars(LeaderAppendEntries(s)) /\ WF_vars(BecomeCandidate(s))

Spec == Init /\ [][Next]_vars

=============================================================================