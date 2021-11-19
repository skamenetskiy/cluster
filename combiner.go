package cluster

import (
	"regexp"
	"strings"
)

// NewCombiner returns a new Combiner.
func NewCombiner(sep string, validator *regexp.Regexp) Combiner {
	return &combiner{strings.TrimSpace(sep), validator}
}

// Combiner interface.
type Combiner interface {
	// Validate shard ID.
	Validate(string) bool

	// Combine id and shardId into a single string.
	Combine(string, string) string

	// Extract id and shardId from a single string.
	Extract(string) (string, string, error)
}

var (
	defaultCombiner = NewCombiner("@", regexp.MustCompile("^[a-zA-Z0-9]{6}$"))
)

type combiner struct {
	sep string
	reg *regexp.Regexp
}

func (c *combiner) Validate(shardId string) bool {
	return c.reg.MatchString(shardId)
}

func (c *combiner) Combine(id string, shardId string) string {
	return strings.TrimSpace(id) + c.sep + strings.TrimSpace(shardId)
}

func (c *combiner) Extract(id string) (string, string, error) {
	id = strings.TrimSpace(id)
	i := strings.LastIndex(id, c.sep)
	if i == -1 {
		return "", "", ErrIdParseFailed
	}
	v := id[:i]
	vs := id[i+1:]
	if !c.Validate(vs) || len(v) == 0 {
		return "", "", ErrIdParseFailed
	}
	return v, vs, nil
}
