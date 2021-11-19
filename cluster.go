package cluster

import (
	"sync/atomic"
)

// NewCluster returns a new Cluster.
func NewCluster(gen Generator, com Combiner, shards ...Shard) (Cluster, error) {
	if gen == nil {
		return nil, cErr("id generator cannot be nil")
	}
	if com == nil {
		com = defaultCombiner
	}
	l := len(shards)
	if l == 0 {
		return nil, cErr("cannot init cluster without shards")
	}
	c := &cluster{
		gen: gen,
		com: com,
		ss:  make([]Shard, l),
		ws:  make([]Shard, 0, l),
		ms:  make(map[string]Shard, l),
	}
	if err := c.validate(shards); err != nil {
		return nil, wrapErr(err, "shard validation failed")
	}
	for i, s := range shards {
		c.ss[i] = s
		c.ms[s.ID()] = s
		if !s.ReadOnly() {
			c.ws = append(c.ws, s)
		}
	}
	return c, nil
}

// Cluster interface.
type Cluster interface {
	// One returns a Shard by item ID.
	One(string) (Shard, error)

	// Many returns a map of Shards with slice of corresponding IDs as value.
	Many(...string) (map[Shard][]string, error)

	// All returns all Shards.
	All() []Shard

	// Next returns a new (generated) ID and corresponding Shard.
	Next() (string, Shard, error)
}

type cluster struct {
	gen Generator
	com Combiner
	ss  []Shard
	ws  []Shard
	ms  map[string]Shard
	n   uint64
}

func (c *cluster) One(id string) (Shard, error) {
	return c.shardById(id)
}

func (c *cluster) Many(ids ...string) (map[Shard][]string, error) {
	res := make(map[Shard][]string)
	for _, id := range ids {
		s, err := c.shardById(id)
		if err != nil {
			return res, err
		}
		if _, exists := res[s]; !exists {
			res[s] = make([]string, 0, len(ids))
		}
		res[s] = append(res[s], id)
	}
	return res, nil
}

func (c *cluster) All() []Shard {
	res := make([]Shard, len(c.ss))
	copy(res, c.ss)
	return res
}

func (c *cluster) Next() (string, Shard, error) {
	if len(c.ws) == 0 {
		return "", nil, ErrNoWritableShard
	}
	s := c.next()
	return c.com.Combine(c.gen.Generate(), s.ID()), s, nil
}

func (c *cluster) next() Shard {
	n := atomic.AddUint64(&c.n, 1)
	return c.ws[(int(n)-1)%len(c.ws)]
}

func (c *cluster) shardById(id string) (Shard, error) {
	_, sid, err := c.com.Extract(id)
	if err != nil {
		return nil, err
	}
	if s, exists := c.ms[sid]; exists {
		return s, nil
	}
	return nil, ErrShardNotFound
}

func (c *cluster) validate(shards []Shard) error {
	uniq := make(map[string]struct{}, len(shards))
	for _, s := range shards {
		if s.ID() == "" {
			return cErr("shard id is empty")
		}
		if !c.com.Validate(s.ID()) {
			return cErr("invalid shard id")
		}
		if s.Conn() == nil {
			return cErr("database connection is nil")
		}
		if _, exists := uniq[s.ID()]; exists {
			return cErr("duplicate shard id '" + s.ID() + "'")
		}
		uniq[s.ID()] = struct{}{}
	}
	return nil
}
