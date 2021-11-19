package cluster

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestNewShard(t *testing.T) {
	type args struct {
		id   string
		conn *sql.DB
		readonly bool
	}
	tests := []struct {
		name string
		args args
		want Shard
	}{
		{
			"",
			args{"", &sql.DB{}, false},
			&shard{id: "", conn: &sql.DB{}, ro: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewShard(tt.args.id, tt.args.conn, tt.args.readonly); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewShard() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shard_Conn(t *testing.T) {
	tests := []struct {
		name     string
		conn   *sql.DB
		want   *sql.DB
	}{
		{"one", &sql.DB{}, &sql.DB{}},
		{"two", &sql.DB{}, &sql.DB{}},
		{"thee", &sql.DB{}, &sql.DB{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard{
				conn:   tt.conn,
			}
			if got := s.Conn(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Conn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shard_ID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want string
	}{
		{"one", "one", "one"},
		{"two", "two", "two"},
		{"three", "three", "three"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard{
				id: tt.id,
			}
			if got := s.ID(); got != tt.want {
				t.Errorf("ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shard_ReadOnly(t *testing.T) {
	tests := []struct {
		name string
		ro   bool
		want bool
	}{
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard{
				ro: tt.ro,
			}
			if got := s.ReadOnly(); got != tt.want {
				t.Errorf("ReadOnly() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_shard_SetReadOnly(t *testing.T) {
	tests := []struct {
		name string
		ro   bool
		want bool
	}{
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
		{"true", true, true},
		{"false", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &shard{}
			s.SetReadOnly(tt.ro)
			if got := s.ro; got != tt.want {
				t.Errorf("SetReadOnly() = %v, want %v", got, tt.want)
			}
		})
	}
}
