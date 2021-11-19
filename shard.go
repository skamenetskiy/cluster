package cluster

import (
	"database/sql"
	"strings"
	"sync"
)

// NewShard returns a new Shard.
func NewShard(name string, conn *sql.DB, readonly bool) Shard {
	return &shard{
		id:   strings.TrimSpace(name),
		conn: conn,
		ro:   readonly,
	}
}

// Shard interface.
type Shard interface {
	// SetReadOnly state of the shard.
	SetReadOnly(bool)

	// ReadOnly returns true if the Shard is in read only mode.
	ReadOnly() bool

	// ID returns the Shard ID.
	ID() string

	// Conn returns the Shard database connection.
	Conn() *sql.DB
}

type shard struct {
	id     string
	conn   *sql.DB
	ro     bool
	roLock sync.RWMutex
}

func (s *shard) SetReadOnly(readonly bool) {
	s.roLock.Lock()
	s.ro = readonly
	s.roLock.Unlock()
}

func (s *shard) ReadOnly() bool {
	s.roLock.RLock()
	defer s.roLock.RUnlock()
	return s.ro
}

func (s *shard) ID() string {
	return s.id
}

func (s *shard) Conn() *sql.DB {
	return s.conn
}
