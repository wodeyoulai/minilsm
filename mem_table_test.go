package mini_lsm

import (
	"encoding/binary"
	"github.com/huandu/skiplist"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"mini_lsm/pb"
	"reflect"
	"sync"
	"testing"
)

func TestMTable_Get(t *testing.T) {
	type args struct {
		key []SklElement
	}

	keyBinary := SklElement{
		Key:       []byte("/config/xx"),
		Version:   0,
		Timestamp: 0,
		Value:     nil,
	}

	log := zap.Logger{}

	w, _ := NewMTableWithWAL(&log, 1, "tmp.wal")

	tests := []struct {
		name   string
		fields *MTable
		args   args
		want   []byte
		want1  bool
	}{
		{
			name:   "basic test",
			fields: w,
			args:   args{key: []SklElement{keyBinary}},
			want:   nil,
			want1:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.fields
			got, got1 := m.Get(keyBinary)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestMTable_ID(t *testing.T) {
	type fields struct {
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if got := m.ID(); got != tt.want {
				t.Errorf("ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMTable_Put(t *testing.T) {
	type args struct {
		key []pb.KeyValue
	}

	testKey := pb.Key{Key: []byte("/config/xx")}
	keyBinary, _ := proto.Marshal(&testKey)

	value := pb.Value{Value: []byte("content")}
	keyBinaryValue, _ := proto.Marshal(&value)

	log := zap.Logger{}
	tests := []struct {
		name   string
		fields *MTable
		args   args
		want   []byte
		want1  bool
	}{
		{
			name:   "basic put test",
			fields: NewMTable(&log, 1, "tmp.wal"),
			args:   args{[]pb.KeyValue{pb.KeyValue{Key: keyBinary, Value: keyBinaryValue}}},
			want:   nil,
			want1:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.fields
			if err := m.Put(&tt.args.key[0]); err != nil {
				t.Errorf("Put() error = %v, wantErr %v", err, false)
			}
		})
	}
}

func TestMTable_PutBatch(t *testing.T) {
	type fields struct {
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}

	type args struct {
		data []pb.KeyValue
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if err := m.PutBatch(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMTable_SyncWAL(t *testing.T) {
	type fields struct {
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if err := m.SyncWAL(); (err != nil) != tt.wantErr {
				t.Errorf("SyncWAL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMTable_readFromWal(t *testing.T) {
	type fields struct {
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			m.readFromWal()
		})
	}
}

func TestNewMTable(t *testing.T) {
	type args struct {
		logger  *zap.Logger
		id      uint64
		walPath string
	}
	tests := []struct {
		name string
		args args
		want *MTable
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMTable(tt.args.logger, tt.args.id, tt.args.walPath); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMTableWithWAL(t *testing.T) {
	type args struct {
		logger *zap.Logger
		id     uint64
		path   string
	}
	tests := []struct {
		name    string
		args    args
		want    *MTable
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMTableWithWAL(tt.args.logger, tt.args.id, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMTableWithWAL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMTableWithWAL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecoverFromWAL(t *testing.T) {
	type args struct {
		id   uint64
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *MTable
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RecoverFromWAL(tt.args.id, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("RecoverFromWAL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RecoverFromWAL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyMarshal(t *testing.T) {
	arg := pb.Key{Key: []byte("xxxx"), Timestamp: 89789798, Version: 9}
	_ = keyMarshal(arg)
	type args struct {
		key pb.Key
	}

	b := make([]byte, 10)
	binary.BigEndian.PutUint64(b, arg.Timestamp)
	res := binary.BigEndian.Uint64(b)
	_ = res
	tests := []struct {
		name string
		args args
		want []byte
	}{
		//{name: "normal", args: , want: },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyMarshal(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyMarshal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyUnMarshal(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name string
		args args
		want pb.Key
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyUnmarshal(tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyUnmarshal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMTable_ApproximateSize(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if got := m.ApproximateSize(); got != tt.want {
				t.Errorf("ApproximateSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMTable_Get1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	type args struct {
		key SklElement
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
		want1  bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			got, got1 := m.Get(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Get() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestMTable_ID1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if got := m.ID(); got != tt.want {
				t.Errorf("ID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMTable_Put1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	type args struct {
		value *pb.KeyValue
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if err := m.Put(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMTable_PutBatch1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	type args struct {
		data []pb.KeyValue
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if err := m.PutBatch(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("PutBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMTable_SyncWAL1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			if err := m.SyncWAL(); (err != nil) != tt.wantErr {
				t.Errorf("SyncWAL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMTable_readFromWal1(t *testing.T) {
	type fields struct {
		mu               sync.Mutex
		skl              *skiplist.SkipList
		id               uint64
		approximate_size uint64
		wal              *Wal
		logger           *zap.Logger
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTable{
				mu:              tt.fields.mu,
				skl:             tt.fields.skl,
				id:              tt.fields.id,
				approximateSize: tt.fields.approximate_size,
				wal:             tt.fields.wal,
				logger:          tt.fields.logger,
			}
			m.readFromWal()
		})
	}
}

func TestMemTableIterator_IsValid(t *testing.T) {
	type fields struct {
		skl  *skiplist.SkipList
		item struct {
			key   []byte
			value []byte
		}
		end []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTableIterator{
				skl:  tt.fields.skl,
				item: tt.fields.item,
				end:  tt.fields.end,
			}
			if got := m.IsValid(); got != tt.want {
				t.Errorf("IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemTableIterator_Key(t *testing.T) {
	type fields struct {
		skl  *skiplist.SkipList
		item struct {
			key   []byte
			value []byte
		}
		end []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTableIterator{
				skl:  tt.fields.skl,
				item: tt.fields.item,
				end:  tt.fields.end,
			}
			if got := m.Key(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Key() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemTableIterator_Next(t *testing.T) {
	type fields struct {
		skl  *skiplist.SkipList
		item struct {
			key   []byte
			value []byte
		}
		end []byte
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTableIterator{
				skl:  tt.fields.skl,
				item: tt.fields.item,
				end:  tt.fields.end,
			}
			if err := m.Next(); (err != nil) != tt.wantErr {
				t.Errorf("Next() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMemTableIterator_NumActiveIterators(t *testing.T) {
	type fields struct {
		skl  *skiplist.SkipList
		item struct {
			key   []byte
			value []byte
		}
		end []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTableIterator{
				skl:  tt.fields.skl,
				item: tt.fields.item,
				end:  tt.fields.end,
			}
			if got := m.NumActiveIterators(); got != tt.want {
				t.Errorf("NumActiveIterators() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMemTableIterator_Value(t *testing.T) {
	type fields struct {
		skl  *skiplist.SkipList
		item struct {
			key   []byte
			value []byte
		}
		end []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MTableIterator{
				skl:  tt.fields.skl,
				item: tt.fields.item,
				end:  tt.fields.end,
			}
			if got := m.Value(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMTable1(t *testing.T) {
	type args struct {
		logger  *zap.Logger
		id      uint64
		walPath string
	}
	tests := []struct {
		name string
		args args
		want *MTable
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMTable(tt.args.logger, tt.args.id, tt.args.walPath); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMTableWithWAL1(t *testing.T) {
	type args struct {
		logger *zap.Logger
		id     uint64
		path   string
	}
	tests := []struct {
		name    string
		args    args
		want    *MTable
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMTableWithWAL(tt.args.logger, tt.args.id, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewMTableWithWAL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMTableWithWAL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecoverFromWAL1(t *testing.T) {
	type args struct {
		id   uint64
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    *MTable
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RecoverFromWAL(tt.args.id, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("RecoverFromWAL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RecoverFromWAL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyMarshal1(t *testing.T) {
	type args struct {
		key pb.Key
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyMarshal(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyMarshal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keyUnMarshal1(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name string
		args args
		want pb.Key
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := keyUnmarshal(tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keyUnmarshal() = %v, want %v", got, tt.want)
			}
		})
	}
}
