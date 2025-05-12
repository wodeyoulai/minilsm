# MiniLSM

MiniLSM is a lightweight, high-performance Log-Structured Merge-Tree (LSM) key-value storage engine implemented in Go. This project is a Go port of the Rust [mini-lsm](https://github.com/skyzh/mini-lsm) implementation, aiming to provide the same functionality with Go's unique strengths.

## Rust Implementation Reference

This project is based on the Rust mini-lsm implementation by skyzh:
- **Original Rust Repository**: [github.com/skyzh/mini-lsm](https://github.com/skyzh/mini-lsm)

The Go implementation follows the same architectural principles while adapting to Go's design patterns and concurrency model.

## Features

- Log-Structured Merge-Tree architecture
- ACID transactions with MVCC (Multi-Version Concurrency Control)
- Efficient SSTable (Sorted String Table) format
- Block-based storage with LRU caching
- Multiple compaction strategies (Leveled, Tiered, SimpleLeveled)
- WAL (Write-Ahead Logging) for durability
- Memory-mapped I/O for performance
- Bloom filters for efficient key lookups
- Customizable storage options

## Getting Started

Here's a basic example of how to initialize the `plsm` engine:

```go
import (
    "log" // Added for basic error logging if zap fails initially

    "github.com/prometheus/client_golang/prometheus"
    "github.com/wodeyoulai/plsm" // Ensure this path matches your new repo name!
    "go.uber.org/zap"
)

func main() { // Examples are often clearer within a function context
    // Initialize logger (handle potential error properly in production)
    logger, err := zap.NewDevelopment()
    if err != nil {
        log.Fatalf("can't initialize zap logger: %v", err)
    }
    defer logger.Sync() // Flushes buffer, if any

    // Create Prometheus registry for metrics collection (optional)
    registry := prometheus.NewRegistry()

    // Define storage path (replace with your actual path)
    storagePath := "/path/to/your/storage"

    // Initialize the LSM storage engine
    lsm, err := plsm.NewPLsm(logger, storagePath, registry, plsm.NewDefaultLsmStorageOptions())
    if err != nil {
        logger.Fatal("Failed to initialize PLsm engine", zap.Error(err))
        // Handle error appropriately (e.g., exit, return error)
    }
    // select {} // Block forever if it's a server

```

## Architecture

MiniLSM is structured around these core components:

- **MemTable**: In-memory storage using skiplist
- **SSTable**: Immutable disk storage format
- **Block**: Storage unit within SSTables
- **Iterator**: Interface for traversing data
- **Compaction**: Background process to optimize storage
- **WAL**: Write-ahead logging for crash recovery
- **MVCC**: Concurrency control mechanism

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- Based on the [Rust mini-lsm implementation](https://github.com/skyzh/mini-lsm) by skyzh
- Inspired by [RocksDB](https://rocksdb.org/) and [LevelDB](https://github.com/google/leveldb)
- Built with [Go](https://golang.org/)
