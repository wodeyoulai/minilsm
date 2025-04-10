package mini_lsm

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"testing"
	"time"
)

// setupLsmForBenchmark 创建一个用于测试的LSM实例
func setupLsmForBenchmark(b *testing.B) (*MiniLsm, string) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "lsm-benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	// 初始化logger
	logger, _ := zap.NewDevelopment()

	// 配置LSM选项
	opts := LsmStorageOptions{
		BlockSize:     4096,
		TargetSSTSize: 4 * 1024 * 1024, // 4MB
		MemTableLimit: 3,
		CompactionOpts: CompactionOptions{
			Strategy: TaskTypeSimple,
			SimpleOpts: &SimpleLeveledCompactionOptions{
				SizeRatioPercent:               200,
				Level0FileNumCompactionTrigger: 4,
				MaxLevels:                      7,
			},
		},
		EnableWAL:    true,
		Serializable: false,
		Levels:       7,
	}

	// 创建LSM实例
	registry := prometheus.NewRegistry()
	lsm, err := NewMiniLsm(logger, tempDir, registry, opts)
	if err != nil {
		os.RemoveAll(tempDir)
		b.Fatalf("Failed to create LSM: %v", err)
	}

	return lsm, tempDir
}

// BenchmarkStorageInnerPut 基础写入性能测试
func BenchmarkStorageInnerPut(b *testing.B) {
	// 创建CPU profile文件
	f, err := os.Create("lsm_cpu.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	// 开始CPU profiling
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Fatal(err)
	}
	defer pprof.StopCPUProfile()
	lsm, tempDir := setupLsmForBenchmark(b)
	defer os.RemoveAll(tempDir)

	// 准备测试数据
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
		values[i] = []byte(fmt.Sprintf("value-%08d", i))
	}

	b.ResetTimer()

	// 执行写入测试
	for i := 0; i < b.N; i++ {
		if err := lsm.inner.put(keys[i], values[i]); err != nil {
			b.Fatalf("Put failed at iteration %d: %v", i, err)
		}
	}

	b.StopTimer()
	// 内存profile（可选）
	mf, err := os.Create("lsm_mem.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer mf.Close()
	runtime.GC() // 强制GC以获得更准确的内存使用情况
	if err := pprof.WriteHeapProfile(mf); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkStorageInnerPutValueSizes 测试不同大小值的写入性能
func BenchmarkStorageInnerPutValueSizes(b *testing.B) {
	valueSizes := []int{16, 128, 1024, 1000, 1000, 4096, 16384}
	//valueSizes := []int{16384}

	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	// 启用锁争用和阻塞分析
	runtime.SetMutexProfileFraction(1)           // 收集所有锁争用事件
	runtime.SetBlockProfileRate(1 * 1000 * 1000) // 每 1ms 的阻塞采样一次

	// 启动 pprof 的 HTTP 服务器
	go func() {
		log.Println("Starting pprof server on :6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// CPU profiling
	cpuFile, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// 确保程序退出前写入堆、锁和阻塞的 profile
	defer func() {
		// 堆分配 profile
		heapFile, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(heapFile)
		heapFile.Close()

		// goroutine profile
		goroutineFile, err := os.Create("goroutine.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("goroutine").WriteTo(goroutineFile, 0)
		goroutineFile.Close()

		// 锁争用 profile
		mutexFile, err := os.Create("mutex.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("mutex").WriteTo(mutexFile, 0)
		mutexFile.Close()

		// 阻塞 profile
		blockFile, err := os.Create("block.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("block").WriteTo(blockFile, 0)
		blockFile.Close()
	}()
	f, _ := os.Create("traceBlock.out")
	trace.Start(f)
	defer trace.Stop()
	for _, size := range valueSizes {
		b.Run(fmt.Sprintf("ValueSize-%dB", size), func(b *testing.B) {
			lsm, tempDir := setupLsmForBenchmark(b)
			defer os.RemoveAll(tempDir)

			// 准备固定大小的值
			value := make([]byte, size)
			for i := 0; i < size; i++ {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {

				key := []byte(fmt.Sprintf("key-size-%d-%08d", size, i))
				if err := lsm.inner.put(key, value); err != nil {
					b.Fatalf("Put failed with value size %d: %v", size, err)
				}
			}

			b.StopTimer()
		})
	}
}
func BenchmarkStorageInnerPutValueSizesNoPProf(b *testing.B) {
	valueSizes := []int{16, 128, 1024, 1000, 1000, 4096, 16384}
	//valueSizes := []int{16384}

	runtime.SetMutexProfileFraction(1)           // 收集所有锁争用事件
	runtime.SetBlockProfileRate(1 * 1000 * 1000) // 每 1ms 的阻塞采样一次

	// 启动 pprof 的 HTTP 服务器
	go func() {
		log.Println("Starting pprof server on :6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// CPU profiling
	cpuFile, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// 确保程序退出前写入堆、锁和阻塞的 profile
	defer func() {
		// 堆分配 profile
		heapFile, err := os.Create("heap.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(heapFile)
		heapFile.Close()

		// goroutine profile
		goroutineFile, err := os.Create("goroutine.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("goroutine").WriteTo(goroutineFile, 0)
		goroutineFile.Close()

		// 锁争用 profile
		mutexFile, err := os.Create("mutex.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("mutex").WriteTo(mutexFile, 0)
		mutexFile.Close()

		// 阻塞 profile
		blockFile, err := os.Create("block.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.Lookup("block").WriteTo(blockFile, 0)
		blockFile.Close()
	}()
	for _, size := range valueSizes {
		b.Run(fmt.Sprintf("ValueSize-%dB", size), func(b *testing.B) {
			lsm, tempDir := setupLsmForBenchmark(b)
			defer os.RemoveAll(tempDir)

			// 准备固定大小的值
			value := make([]byte, size)
			for i := 0; i < size; i++ {
				value[i] = byte(i % 256)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {

				key := []byte(fmt.Sprintf("key-size-%d-%08d", size, i))
				if err := lsm.inner.put(key, value); err != nil {
					b.Fatalf("Put failed with value size %d: %v", size, err)
				}
			}

			b.StopTimer()
		})
	}
}

// BenchmarkStorageInnerPutSequential 测试顺序写入性能
func BenchmarkStorageInnerPutSequential(b *testing.B) {
	lsm, tempDir := setupLsmForBenchmark(b)
	defer os.RemoveAll(tempDir)

	b.ResetTimer()

	// 顺序写入
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("seq-%08d", i))
		value := []byte(fmt.Sprintf("val-%08d", i))

		if err := lsm.inner.put(key, value); err != nil {
			b.Fatalf("Sequential put failed: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkStorageInnerPutRandom 测试随机键写入性能
func BenchmarkStorageInnerPutRandom(b *testing.B) {
	lsm, tempDir := setupLsmForBenchmark(b)
	defer os.RemoveAll(tempDir)

	// 设置随机种子
	rand.Seed(time.Now().UnixNano())

	// 生成随机键
	const keyspace = 1000000 // 键空间大小
	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		randomKey := rand.Intn(keyspace)
		keys[i] = []byte(fmt.Sprintf("rnd-%08d", randomKey))
		values[i] = []byte(fmt.Sprintf("val-%08d", i))
	}

	b.ResetTimer()

	// 执行随机写入
	for i := 0; i < b.N; i++ {
		if err := lsm.inner.put(keys[i], values[i]); err != nil {
			b.Fatalf("Random put failed: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkStorageInnerPutOverwrite 测试覆盖写入性能
func BenchmarkStorageInnerPutOverwrite(b *testing.B) {
	lsm, tempDir := setupLsmForBenchmark(b)
	defer os.RemoveAll(tempDir)

	// 定义写入的键数量
	numKeys := 1000
	if b.N < numKeys {
		numKeys = b.N
	}

	// 预先写入所有键
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("overwrite-key-%04d", i))
		value := []byte(fmt.Sprintf("original-value-%04d", i))

		if err := lsm.inner.put(key, value); err != nil {
			b.Fatalf("Initial write failed: %v", err)
		}
	}

	b.ResetTimer()

	// 执行覆盖写入
	for i := 0; i < b.N; i++ {
		keyIdx := i % numKeys
		key := []byte(fmt.Sprintf("overwrite-key-%04d", keyIdx))
		value := []byte(fmt.Sprintf("updated-value-%04d-%04d", keyIdx, i))

		if err := lsm.inner.put(key, value); err != nil {
			b.Fatalf("Overwrite failed: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkStorageInnerPutTransaction 测试事务写入性能
func BenchmarkStorageInnerPutTransaction(b *testing.B) {
	lsm, tempDir := setupLsmForBenchmark(b)
	defer os.RemoveAll(tempDir)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// 创建写事务
		tx := NewWriteTx(lsm.inner, lsm.mvcc, false)

		// 执行写入
		key := []byte(fmt.Sprintf("tx-key-%08d", i))
		value := []byte(fmt.Sprintf("tx-value-%08d", i))

		if err := tx.Put(key, value); err != nil {
			b.Fatalf("Transaction put failed: %v", err)
		}

		// 提交事务
		if err := tx.Commit(); err != nil {
			b.Fatalf("Transaction commit failed: %v", err)
		}
	}

	b.StopTimer()
}

// BenchmarkStorageInnerBatchWrite 测试批量写入性能
func BenchmarkStorageInnerBatchWrite(b *testing.B) {
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			lsm, tempDir := setupLsmForBenchmark(b)
			defer os.RemoveAll(tempDir)

			// 为每个批次准备足够的键值对
			keysPerBatch := make([][]byte, batchSize)
			valuesPerBatch := make([][]byte, batchSize)

			b.ResetTimer()

			// 对每个批次执行b.N次
			for i := 0; i < b.N; i++ {
				// 生成本批次的键值对
				for j := 0; j < batchSize; j++ {
					keysPerBatch[j] = []byte(fmt.Sprintf("batch-%d-key-%04d", i, j))
					valuesPerBatch[j] = []byte(fmt.Sprintf("batch-%d-value-%04d", i, j))
				}

				// 创建写事务
				tx := NewWriteTx(lsm.inner, lsm.mvcc, false)

				// 批量写入
				for j := 0; j < batchSize; j++ {
					if err := tx.Put(keysPerBatch[j], valuesPerBatch[j]); err != nil {
						b.Fatalf("Batch put failed: %v", err)
					}
				}

				// 提交事务
				if err := tx.Commit(); err != nil {
					b.Fatalf("Batch commit failed: %v", err)
				}
			}

			b.StopTimer()
		})
	}
}
