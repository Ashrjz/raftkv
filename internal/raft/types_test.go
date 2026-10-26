package raft

import (
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		ID:                 1,
		Peers:              map[NodeID]string{1: "a", 2: "b", 3: "c"},
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
	}
}

func TestConfigValidate(t *testing.T) {
	if err := validConfig().validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero ID", func(c *Config) { c.ID = 0 }},
		{"self not in peers", func(c *Config) { c.ID = 9 }},
		{"zero peer ID", func(c *Config) { c.Peers[0] = "x" }},
		{"zero min timeout", func(c *Config) { c.ElectionTimeoutMin = 0 }},
		{"max equals min", func(c *Config) { c.ElectionTimeoutMax = c.ElectionTimeoutMin }},
		{"zero heartbeat", func(c *Config) { c.HeartbeatInterval = 0 }},
		{"heartbeat above half of min", func(c *Config) { c.HeartbeatInterval = 100 * time.Millisecond }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(&c)
			if err := c.validate(); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
	// Boundary: exactly half is allowed.
	c := validConfig()
	c.HeartbeatInterval = 75 * time.Millisecond
	if err := c.validate(); err != nil {
		t.Fatalf("heartbeat == min/2 should be valid: %v", err)
	}
}

func TestRoleString(t *testing.T) {
	for r, want := range map[Role]string{Follower: "follower", Candidate: "candidate", Leader: "leader"} {
		if got := r.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", int(r), got, want)
		}
	}
}
