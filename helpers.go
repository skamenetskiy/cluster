package cluster

const (
	ErrShardNotFound   = cErr("shard not found")
	ErrNoWritableShard = cErr("could not find a writable shard")
	ErrIdParseFailed   = cErr("failed to parse id")


)

// Generator interface.
type Generator interface {

	// Generate a new ID.
	Generate() string
}



type cErr string

func (err cErr) Error() string {
	return string(err)
}

func wrapErr(e error, p string) error {
	return cErr(p + ": " + e.Error())
}
//
//func extract(id string) (string, error) {
//	id = strings.TrimSpace(id)
//	i := strings.LastIndex(id, shardIdSep)
//	if i == -1 {
//		return "", ErrIdParseFailed
//	}
//	v := id[i+1:]
//	if len(v) != len {
//		return "", ErrIdParseFailed
//	}
//	return v, nil
//}


