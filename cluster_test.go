package cluster

import (
	"database/sql"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
)

var (
	testIdGen = &tig{100000}
)

type tig struct {
	i uint64
}

func (t *tig) Generate() string {
	atomic.AddUint64(&t.i, 1)
	id := atomic.LoadUint64(&t.i)
	return strconv.FormatUint(id, 16)
}

func TestNewCluster(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, true),
		NewShard("000003", &sql.DB{}, true),
	}
	badShards := []Shard{
		NewShard("1", nil, true),
		NewShard("2", nil, true),
		NewShard("3", nil, true),
	}
	type args struct {
		idGen  Generator
		com    Combiner
		shards []Shard
	}
	tests := []struct {
		name    string
		args    args
		want    Cluster
		wantErr bool
	}{
		{"ok", args{testIdGen, defaultCombiner, shards},
			&cluster{
				testIdGen,
				defaultCombiner,
				append(make([]Shard, 0, 3), shards...),
				append(make([]Shard, 0, 3), shards[0]),
				map[string]Shard{
					"000001": shards[0],
					"000002": shards[1],
					"000003": shards[2],
				},
				0,
			}, false},
		{"ok without combiner", args{testIdGen, nil, shards},
			&cluster{
				testIdGen,
				defaultCombiner,
				append(make([]Shard, 0, 3), shards...),
				append(make([]Shard, 0, 3), shards[0]),
				map[string]Shard{
					"000001": shards[0],
					"000002": shards[1],
					"000003": shards[2],
				},
				0,
			}, false},
		{"no idGen", args{nil, defaultCombiner, nil}, nil, true},
		{"no shards", args{testIdGen, defaultCombiner, nil}, nil, true},
		{"validation error", args{testIdGen, defaultCombiner, badShards}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewCluster(tt.args.idGen, tt.args.com, tt.args.shards...)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCluster() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewCluster() got = %v, want %v", got, tt.want)
				return
			}
		})
	}
}

func Test_cluster_All(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, true),
		NewShard("000003", &sql.DB{}, true),
	}
	tests := []struct {
		name string
		want []Shard
	}{
		{"one", shards},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCluster(testIdGen, defaultCombiner, shards...)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.All(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("All() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Many(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, true),
		NewShard("000003", &sql.DB{}, true),
	}
	c, err := NewCluster(testIdGen, defaultCombiner, shards...)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		name    string
		ids     []string
		want    map[Shard][]string
		wantErr bool
	}{
		{
			"ok",
			[]string{
				"100@000001",
				"100@000002",
				"100@000003",
				"200@000001",
				"200@000002",
				"200@000003",
				"300@000001",
				"300@000002",
				"300@000003",
				"400@000001",
				"400@000002",
				"400@000003",
				"500@000001",
				"500@000002",
				"500@000003",
			},
			map[Shard][]string{
				shards[0]: {
					"100@000001",
					"200@000001",
					"300@000001",
					"400@000001",
					"500@000001",
				},
				shards[1]: {
					"100@000002",
					"200@000002",
					"300@000002",
					"400@000002",
					"500@000002",
				},
				shards[2]: {
					"100@000003",
					"200@000003",
					"300@000003",
					"400@000003",
					"500@000003",
				},
			},
			false,
		},
		{
			"fail",
			[]string{
				"100@000001",
				"200@000001",
				"300@000001",
				"400@000001",
				"500@000001",
				"100@000004",
			},
			map[Shard][]string{
				shards[0]: {
					"100@000001",
					"200@000001",
					"300@000001",
					"400@000001",
					"500@000001",
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.Many(tt.ids...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Many() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Many() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_Next(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, false),
		NewShard("000003", &sql.DB{}, false),
	}
	c1, err := NewCluster(testIdGen, defaultCombiner, shards...)
	if err != nil {
		t.Error(err)
		return
	}
	roShards := []Shard{
		NewShard("000001", &sql.DB{}, true),
	}
	c2, err := NewCluster(testIdGen, defaultCombiner, roShards...)
	if err != nil {
		t.Error(err)
		return
	}
	tests := []struct {
		name    string
		cluster Cluster
		want    string
		want1   Shard
		wantErr bool
	}{
		{"", c1, "186a1@000001", shards[0], false},
		{"", c1, "186a2@000002", shards[1], false},
		{"", c1, "186a3@000003", shards[2], false},
		{"", c2, "", nil, true},
		{"", c2, "", nil, true},
		{"", c2, "", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.cluster.Next()
			if (err != nil) != tt.wantErr {
				t.Errorf("Next() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Next() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Next() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_cluster_One(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, true),
		NewShard("000003", &sql.DB{}, true),
	}
	c, err := NewCluster(testIdGen, defaultCombiner, shards...)
	if err != nil {
		t.Error(err)
		return
	}
	type args struct {
		id string
	}
	tests := []struct {
		name string
		//c Cluster
		args    args
		want    Shard
		wantErr bool
	}{
		{"100@000001", args{"100@000001"}, shards[0], false},
		{"101@000001", args{"101@000001"}, shards[0], false},
		{"102@000001", args{"102@000001"}, shards[0], false},
		{"100@000001", args{"100@000001"}, shards[0], false},
		{"101@000002", args{"101@000002"}, shards[1], false},
		{"102@000003", args{"102@000003"}, shards[2], false},
		{"102@000004", args{"102@000004"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.One(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("One() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("One() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_next(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, false),
		NewShard("000003", &sql.DB{}, false),
	}
	c, err := NewCluster(testIdGen, defaultCombiner, shards...)
	if err != nil {
		t.Error(err)
		return
	}
	cl := c.(*cluster)
	tests := []struct {
		name string
		want Shard
	}{
		{"000001", shards[0]},
		{"000002", shards[1]},
		{"000003", shards[2]},
		{"000001", shards[0]},
		{"000002", shards[1]},
		{"000003", shards[2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cl.next(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_shardById(t *testing.T) {
	shards := []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, false),
		NewShard("000003", &sql.DB{}, false),
	}
	c, err := NewCluster(testIdGen, defaultCombiner, shards...)
	if err != nil {
		t.Error(err)
		return
	}
	cl := c.(*cluster)
	type args struct {
		id string
	}
	tests := []struct {
		name    string
		args    args
		want    Shard
		wantErr bool
	}{
		{"100@000001", args{"100@000001"}, shards[0], false},
		{"100@000002", args{"100@000002"}, shards[1], false},
		{"100@000003", args{"100@000003"}, shards[2], false},
		{"100@000004", args{"100@000004"}, nil, true},
		{"10", args{"10"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cl.shardById(tt.args.id)
			if (err != nil) != tt.wantErr {
				t.Errorf("shardById() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("shardById() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cluster_validate(t *testing.T) {
	c, err := NewCluster(testIdGen, defaultCombiner, []Shard{
		NewShard("000001", &sql.DB{}, false),
		NewShard("000002", &sql.DB{}, true),
		NewShard("000003", &sql.DB{}, true),
	}...)
	if err != nil {
		t.Error(err)
		return
	}
	cl := c.(*cluster)
	type args struct {
		shards []Shard
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"one",
			args{
				[]Shard{
					NewShard("000001", &sql.DB{}, false),
					NewShard("000002", &sql.DB{}, true),
					NewShard("000003", &sql.DB{}, true),
				},
			},
			false,
		},
		{
			"duplicate shard id",
			args{
				[]Shard{
					NewShard("000001", &sql.DB{}, false),
					NewShard("000001", &sql.DB{}, false),
				},
			},
			true,
		},
		{
			"empty shard id",
			args{
				[]Shard{
					NewShard("", &sql.DB{}, false),
				},
			},
			true,
		},
		{
			"invalid shard id",
			args{
				[]Shard{
					NewShard("1", &sql.DB{}, false),
				},
			},
			true,
		},
		{
			"nil database connection",
			args{
				[]Shard{
					NewShard("000001", nil, false),
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := cl.validate(tt.args.shards); (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
