<p align="center">
  <img  width="200" src="https://github.com/user-attachments/assets/3bb7c1cc-5c97-4755-aa12-dfa85bc91344" alt="Centered Image"/>
  <h1 align="center">Baggins DB</h1>
</p>

<p align="center">
  Baggins DB is a simple Cassandra-inspired key-value store. While not production-ready, it serves as an educational project to explore low-level database internals, concurrency control, and performance tuning techniques.
</p>

<p align="center">

  <img src="https://github.com/flynnfc/bagginsdb/actions/workflows/build.yml/badge.svg" alt="Build badge">

  <a href="https://github.com/flynnfc/BagginsDB/blob/main/LICENSE.md">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT" title="MIT License" />
  </a>


</p>

---

## Features

- **Memtable (In-Memory Index):**  
  Stores recently written data in a sorted skiplist for quick insertion and retrieval. Once it reaches a certain size threshold, it is flushed to disk as an immutable SSTable.

- **SSTables (On-Disk Storage):**  
  Writes are organized into append-only, immutable files known as SSTables. Each SSTable is sorted by key and includes:
  - A Bloom filter to quickly determine if a key might exist.
  - A sparse index to jump near the desired key without scanning the entire file.
- **Compaction:**  
  Over time, multiple SSTables are merged and deduplicated into a single larger SSTable. This process, known as compaction, reduces storage fragmentation and keeps read performance stable by limiting the number of SSTables that must be searched.

- **TrueTime Integration (Mocked):**  
  The code incorporates a `truetime` component, simulating reliable timestamp generation, similar in spirit to [Google’s TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency), though far simpler and not distributed. This allows the system to track record versions and choose the newest value during compactions.

## Project Structure

- `internal/database/`  
  Contains the core database logic including:

  - `database.go`: The `Database` struct that ties together memtables, SSTableManager, and timing.
  - `memtable.go`: In-memory skiplist for quick writes and reads.
  - `sstable.go`, `sstable_manager.go`: Handling on-disk SSTables, building them from memtables, indexing, and merging them during compaction.

- `internal/truetime/`  
  Mock time service that provides timestamps for record inserts.

- `simulation/`  
  Contains load-testing and simulation scripts that run various scenarios:
  - Bulk inserts and reads.
  - Mixed workloads (reads/writes).
  - Realistic Cassandra-like loads with random keys, values, and read/write ratios.
- `logger/`  
  A simple logging wrapper configured to produce structured logs via `zap`.

## Concurrency and Locking

The database uses Go’s concurrency features. The `Database` struct employs a `sync.RWMutex` to allow concurrent reads and exclusive writes. However, the underlying SSTable code may require additional improvements, particularly around disk I/O operations.

## Troubleshooting

- **Invalid Key Length Errors:**  
  Typically caused by concurrent reads stepping on each other’s file position. Ensure proper synchronization around SSTable file access.

- **High Lock Contention:**  
  If `Get` operations block due to a locked `Put` or vice versa, consider using more fine-grained locking or `RWMutex` to allow multiple readers simultaneously.

- **Performance Tuning:**
  - Adjust the Bloom filter size to reduce false positives.
  - Tune the index interval in SSTables.
  - Increase memtable flush thresholds if you have enough memory.

## Roadmap

- Add delete support (tombstones).
- Implement proper error handling and recovery after crashes.
- Add network interface or gRPC endpoint for remote access.
- Enhance compaction strategies to handle large-scale data better.
- Integrate more benchmarks and profiling tools to guide optimizations.

## License

This project is distributed under the MIT License. See `LICENSE` for details.

---

This README provides an overview, setup instructions, guidance on using and testing the database, as well as insights into its internal design and areas for future improvement. You can modify the specifics to match your actual codebase and project goals.
