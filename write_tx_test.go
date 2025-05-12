package plsm

import (
	"bytes"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wodeyoulai/plsm/pb"
	"go.uber.org/zap"
	_ "net/http/pprof"
	"os"
	"runtime/trace"
	"sync"
	"testing"
	"time"
)

func TestWriteTxBasic(t *testing.T) {
	// 设置测试环境
	logger, _ := zap.NewDevelopment()
	tempDir := t.TempDir()

	// 创建LSM存储引擎
	opts := LsmStorageOptions{
		BlockSize:     4096,
		TargetSSTSize: 1024 * 1024, // 1MB
		MemTableLimit: 2,
		EnableWAL:     true,
		Serializable:  true,
		Levels:        7,
	}

	registry := prometheus.NewRegistry()

	lsm, err := NewPLsm(logger, tempDir, registry, opts)
	if err != nil {
		t.Fatalf("Failed to create PLsm: %v", err)
	}
	defer lsm.Close()

	// 测试基本的写入操作
	t.Run("BasicPut", func(t *testing.T) {
		// 获取写事务
		tx := NewWriteTx(lsm.inner, lsm.mvcc, true)

		// 写入一些键值对
		key := []byte("test-key-1")
		value := []byte("test-value-1")

		err := tx.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// 提交事务
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// 验证写入是否成功
		readValue, err := lsm.Get(key)
		if err != nil {
			t.Fatalf("Get after commit failed: %v", err)
		}

		if !bytes.Equal(readValue, value) {
			t.Errorf("Expected value %v, got %v", value, readValue)
		}
	})

	// 测试多键写入
	t.Run("MultipleKeys", func(t *testing.T) {
		tx := NewWriteTx(lsm.inner, lsm.mvcc, true)

		keys := [][]byte{
			[]byte("multi-key-1"),
			[]byte("multi-key-2"),
			[]byte("multi-key-3"),
		}

		values := [][]byte{
			[]byte("multi-value-1"),
			[]byte("multi-value-2"),
			[]byte("multi-value-3"),
		}

		// 写入多个键值对
		for i := range keys {
			err := tx.Put(keys[i], values[i])
			if err != nil {
				t.Fatalf("Put for key %s failed: %v", keys[i], err)
			}
		}

		// 提交事务
		err := tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// 验证所有键值对是否写入成功
		for i := range keys {
			readValue, err := lsm.Get(keys[i])
			if err != nil {
				t.Fatalf("Get for key %s failed: %v", keys[i], err)
			}

			if !bytes.Equal(readValue, values[i]) {
				t.Errorf("For key %s: expected value %v, got %v", keys[i], values[i], readValue)
			}
		}
	})

	// 测试事务回滚
	t.Run("Rollback", func(t *testing.T) {
		tx := NewWriteTx(lsm.inner, lsm.mvcc, true)

		key := []byte("rollback-key")
		value := []byte("rollback-value")

		err := tx.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// 回滚事务
		tx.Rollback()

		// 验证键值对是否未被写入
		_, err = lsm.Get(key)
		if err == nil {
			t.Errorf("Key should not exist after rollback, but it does")
		}
	})

	// 测试删除操作
	t.Run("DeleteOperation", func(t *testing.T) {
		// 先插入一个键值对
		keyToDelete := []byte("delete-me")
		valueForDelete := []byte("delete-value")

		// 使用一个事务插入
		insertTx := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err := insertTx.Put(keyToDelete, valueForDelete)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		err = insertTx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// 验证插入成功
		readValue, err := lsm.Get(keyToDelete)
		if err != nil {
			t.Fatalf("Get after insert failed: %v", err)
		}
		if !bytes.Equal(readValue, valueForDelete) {
			t.Errorf("Expected value %v, got %v", valueForDelete, readValue)
		}

		// 使用另一个事务删除
		deleteTx := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err = deleteTx.Delete(keyToDelete)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		err = deleteTx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// 验证删除成功
		_, err = lsm.Get(keyToDelete)
		if err == nil {
			t.Errorf("Key should be deleted, but it still exists")
		}
	})

	// 测试事务冲突
	t.Run("TransactionConflict", func(t *testing.T) {
		if !opts.Serializable {
			t.Skip("Skipping test as serializable mode is disabled")
		}

		conflictKey := []byte("conflict-key")
		value1 := []byte("value-1")
		value2 := []byte("value-2")

		// 第一个事务写入
		tx1 := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err := tx1.Put(conflictKey, value1)
		if err != nil {
			t.Fatalf("Put in tx1 failed: %v", err)
		}

		// 提交第一个事务
		err = tx1.Commit()
		if err != nil {
			t.Fatalf("Commit tx1 failed: %v", err)
		}

		// 第二个事务尝试写入同一个键
		tx2 := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err = tx2.Put(conflictKey, value2)
		if err != nil {
			t.Fatalf("Put in tx2 failed: %v", err)
		}

		// 修改第一个事务的值
		tx3 := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err = tx3.Put(conflictKey, []byte("modified-value"))
		if err != nil {
			t.Fatalf("Put in tx3 failed: %v", err)
		}
		err = tx3.Commit()
		if err != nil {
			t.Fatalf("Commit tx3 failed: %v", err)
		}

		// 尝试提交第二个事务，应该失败
		err = tx2.Commit()
		if err == nil {
			t.Errorf("Expected conflict error, but commit succeeded")
		}
		if err != ErrConflict {
			t.Errorf("Expected ErrConflict, got %v", err)
		}
	})

	// 测试关闭的事务操作
	t.Run("ClosedTransactionOperations", func(t *testing.T) {
		tx := NewWriteTx(lsm.inner, lsm.mvcc, true)

		// 执行操作
		key := []byte("closed-tx-key")
		value := []byte("closed-tx-value")

		err := tx.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		// 提交事务
		err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// 尝试在已关闭的事务上执行操作
		err = tx.Put([]byte("another-key"), []byte("another-value"))
		if err == nil {
			t.Errorf("Expected error when putting to closed transaction, but got nil")
		}
		if err != ErrTransactionClosed {
			t.Errorf("Expected ErrTransactionClosed, got %v", err)
		}

		err = tx.Commit()
		if err == nil {
			t.Errorf("Expected error when committing closed transaction, but got nil")
		}
		if err != ErrTransactionClosed {
			t.Errorf("Expected ErrTransactionClosed, got %v", err)
		}
	})

	// 测试大型键值对
	t.Run("LargeKeyValues", func(t *testing.T) {
		// 创建大型值
		largeValue := make([]byte, 1024*1024) // 1MB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		largeKey := []byte("large-value-key")

		tx := NewWriteTx(lsm.inner, lsm.mvcc, true)
		err := tx.Put(largeKey, largeValue)
		if err != nil {
			t.Fatalf("Put large value failed: %v", err)
		}

		err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit with large value failed: %v", err)
		}

		// 验证大型值
		readValue, err := lsm.Get(largeKey)
		if err != nil {
			t.Fatalf("Get large value failed: %v", err)
		}

		if !bytes.Equal(readValue, largeValue) {
			t.Errorf("Large value mismatch")
		}
	})

	// 测试并发事务
	t.Run("ConcurrentTransactions", func(t *testing.T) {
		f, err := os.Create("ConcurrentTransactions.trace")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		if err := trace.Start(f); err != nil {
			t.Fatal(err)
		}
		defer trace.Stop()

		const numGoroutines = 10
		const keysPerGoroutine = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines * keysPerGoroutine)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				//defer wg.Done()

				for i := 0; i < keysPerGoroutine; i++ {
					key := []byte(fmt.Sprintf("conc-key-%d-%d", goroutineID, i))
					value := []byte(fmt.Sprintf("conc-value-%d-%d", goroutineID, i))
					go func() {
						defer wg.Done()
						tx := NewWriteTx(lsm.inner, lsm.mvcc, true)
						//fmt.Printf("id %d,g:%d,get %d ts\n", i, g, tx.readTs)

						err := tx.Put(key, value)
						if err != nil {
							t.Errorf("Put failed in goroutine %d: %v", goroutineID, err)
							return
						}

						//fmt.Printf("id %d,g:%d,committs %d ts\n", i, g, tx.commitTs)

						err = tx.Commit()
						if err != nil {
							t.Errorf("Commit failed in goroutine %d: %v", goroutineID, err)
						}
					}()

				}
			}(g)
		}

		wg.Wait()

		// 验证所有键值对
		for g := 0; g < numGoroutines; g++ {
			for i := 0; i < keysPerGoroutine; i++ {
				key := []byte(fmt.Sprintf("conc-key-%d-%d", g, i))
				expectedValue := []byte(fmt.Sprintf("conc-value-%d-%d", g, i))

				readValue, err := lsm.Get(key)
				if err != nil {
					t.Errorf("Get failed for key generated by goroutine %d: %v,%s", g, err, key)
					continue
				}

				if !bytes.Equal(readValue, expectedValue) {
					t.Errorf("For key from goroutine %d: expected value %s, got %s",
						g, expectedValue, readValue)
				}
			}
		}
	})
}

// 辅助函数，用于创建具有特定属性的Value
func createValue(value []byte, isDeleted bool) *pb.Value {
	return &pb.Value{
		Value:          value,
		CreateRevision: uint64(time.Now().UnixNano()),
		ModRevision:    uint64(time.Now().UnixNano()),
		Version:        1,
		IsDeleted:      isDeleted,
		LeaseId:        0,
	}
}
