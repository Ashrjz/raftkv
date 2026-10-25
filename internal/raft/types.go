package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// NodeID identifies a node. It is non-zero: 0 means "none" (for example
// votedFor = 0 is "no vote this term").
type NodeID uint64

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// LogEntry is one replicated command. Unused until log replication; it is
// declared now so the RPC types match Figure 2 and the wire format won't change.
type LogEntry struct {
	Term    uint64
	Command []byte
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry // empty for a heartbeat
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// Config is the static configuration of one node.
type Config struct {
	ID                 NodeID
	Peers              map[NodeID]string // static list, including self
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	Rand               *rand.Rand // seeded in tests; nil means a time-seeded default (set in New)
}

func (c Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft: config: ID must be non-zero")
	}
	if _, ok := c.Peers[c.ID]; !ok {
		return fmt.Errorf("raft: config: Peers must include self (ID %d)", c.ID)
	}
	for id := range c.Peers {
		if id == 0 {
			return errors.New("raft: config: peer ID must be non-zero")
		}
	}
	if c.ElectionTimeoutMin <= 0 {
		return errors.New("raft: config: ElectionTimeoutMin must be positive")
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		return errors.New("raft: config: ElectionTimeoutMax must exceed ElectionTimeoutMin")
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("raft: config: HeartbeatInterval must be positive")
	}
	// Heartbeat "well below" the minimum election timeout: at least 2 heartbeats
	// must fit in the shortest timeout, or one lost heartbeat triggers an election.
	if c.HeartbeatInterval*2 > c.ElectionTimeoutMin {
		return errors.New("raft: config: HeartbeatInterval must be at most half of ElectionTimeoutMin")
	}
	return nil
}
