package cluster

import (
	"errors"
	"testing"
)

func Test_cErr_Error(t *testing.T) {
	tests := []struct {
		name string
		err  cErr
		want string
	}{
		{"one", cErr("one"), "one"},
		{"two", cErr("two"), "two"},
		{"three", cErr("three"), "three"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func Test_extract(t *testing.T) {
//	type args struct {
//		id string
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    string
//		wantErr bool
//	}{
//		{"000001", args{"abcdefghi@000001"}, "000001", false},
//		{"000002", args{"abcdefghi@000002"}, "000002", false},
//		{"000003", args{"abcdefghi@000003"}, "000003", false},
//		{"000004", args{"abcdefghi@00004"}, "", true},
//		{"000005", args{"00005"}, "", true},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := extract(tt.args.id)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("extract() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if got != tt.want {
//				t.Errorf("extract() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

//func Test_validate(t *testing.T) {
//	type args struct {
//		shards []Shard
//	}
//	tests := []struct {
//		name    string
//		args    args
//		wantErr bool
//	}{
//		{
//			"one",
//			args{
//				[]Shard{
//					NewShard("000001", &sql.DB{}, false),
//					NewShard("000002", &sql.DB{}, true),
//					NewShard("000003", &sql.DB{}, true),
//				},
//			},
//			false,
//		},
//		{
//			"duplicate shard id",
//			args{
//				[]Shard{
//					NewShard("000001", &sql.DB{}, false),
//					NewShard("000001", &sql.DB{}, false),
//				},
//			},
//			true,
//		},
//		{
//			"empty shard id",
//			args{
//				[]Shard{
//					NewShard("", &sql.DB{}, false),
//				},
//			},
//			true,
//		},
//		{
//			"invalid shard id",
//			args{
//				[]Shard{
//					NewShard("1", &sql.DB{}, false),
//				},
//			},
//			true,
//		},
//		{
//			"nil database connection",
//			args{
//				[]Shard{
//					NewShard("000001", nil, false),
//				},
//			},
//			true,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if err := validate(tt.args.shards); (err != nil) != tt.wantErr {
//				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}

func Test_wrapErr(t *testing.T) {
	type args struct {
		e error
		p string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"hello world", args{errors.New("world"), "hello"}, "hello: world"},
		{"numbers", args{errors.New("2"), "1"}, "1: 2"},
		{"longer", args{errors.New("some error message"), "unknown error"},
			"unknown error: some error message"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := wrapErr(tt.args.e, tt.args.p); err.Error() != tt.want {
				t.Errorf("wrapErr() error = %v, wantErr %v", err, tt.want)
			}
		})
	}
}
