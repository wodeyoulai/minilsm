package plsm

import (
	"bytes"
	"fmt"
	"github.com/huandu/skiplist"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func TestNewWal(t *testing.T) {
	type args struct {
		logger  *zap.Logger
		path    string
		options WalOptions
	}
	tests := []struct {
		name string
		args args
		want *Wal
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewWal(tt.args.logger, tt.args.path, DefaultWalOptions()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWalWrite(t *testing.T) {
	l := zap.Logger{}
	w := NewWal(&l, "tmp1", DefaultWalOptions())

	w.Write(1, []byte("xxx"))
	w.Write(2, []byte("yyy"))
	w.Close()
}

func TestSklBytesSqe(t *testing.T) {
	skl := skiplist.New(skiplist.LessThanFunc(func(k1, k2 interface{}) int {
		k1E, k2E := k1.(SklElement), k2.(SklElement)
		res := bytes.Compare(k1E.Key, k2E.Key)
		if res != 0 {
			return -res
		}
		if k1E.Version < k2E.Version {
			return 1
		} else if k1E.Version > k2E.Version {
			return -1
		}
		return 0
	}))

	keyA := SklElement{
		Key:       []byte("/aa"),
		Version:   0,
		Timestamp: 0,
		Value:     nil,
	}
	skl.Set(keyA, "xx")

	keyAa := SklElement{
		Key:       []byte("/aa"),
		Version:   1,
		Timestamp: 0,
		Value:     nil,
	}
	skl.Set(keyAa, "xxa")
	keyB := SklElement{
		Key:       []byte("/xb"),
		Version:   0,
		Timestamp: 0,
		Value:     nil,
	}
	skl.Set(keyB, "yy")

	keyC := SklElement{
		Key:       []byte("/aaa"),
		Version:   0,
		Timestamp: 0,
		Value:     nil,
	}
	skl.Set(keyC, "zz")

	keyD := SklElement{
		Key:       []byte("/ab"),
		Version:   0,
		Timestamp: 0,
		Value:     nil,
	}
	skl.Set(keyD, "aa")

	item := skl.Find(keyC)

	fmt.Printf("%v", item.Value)
	for i := 0; i < 10; i++ {
		t := item.Next()
		if t == nil {
			fmt.Printf("%s", "end")
			break
		}
		fmt.Printf("%v", t.Value)
		item = t
	}
}
