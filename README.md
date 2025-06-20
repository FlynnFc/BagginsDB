<p align="center">
  <img  width="200" src="https://github.com/user-attachments/assets/3bb7c1cc-5c97-4755-aa12-dfa85bc91344" alt="Centered Image"/>
  <h1 align="center">Baggins DB</h1>
</p>

<p align="center">
  Baggins DB is a simple Cassandra-inspired wide-column db. While not production-ready, it serves as an educational project to explore low-level database internals, concurrency control, and performance-tuning techniques.
  <br/>
  <a href="https://flynnfc.dev/work/bagginsdb">
    Learn more on how it's made here
  </a>
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
  Writes are organised into append-only, immutable files known as SSTables. Each SSTable is sorted by key and includes:
  - A Bloom filter to quickly determine if a key might exist.
  - A sparse index to jump near the desired key without scanning the entire file.
- **Compaction:**  
  Over time, multiple SSTables are merged and deduplicated into a single larger SSTable. This process, known as compaction, reduces storage fragmentation and stabilises read performance by limiting the number of SSTables that must be searched.

- **TrueTime Integration (Mocked):**  
  The code incorporates a `truetime` component, simulating reliable timestamp generation, similar in spirit to [Google’s TrueTime](https://cloud.google.com/spanner/docs/true-time-external-consistency), though far simpler and not distributed. This allows the program to avoid distributed locks and choose the newest value during compactions.

## Project Structure

- `pkg/bagginsdb/db`  
  Contains the core database logic, including:

  - `database.go`: The `Database` struct that ties together mem-tables, SSTableManager, and timing.
  - `memtable.go`, `skiplist.go`: In-memory skiplist for quick writes and reads.
  - `sstable.go`, `sstable_manager.go`: Handling on-disk SSTables, building them from memtables, indexing, and merging them during compaction.

- `pkg/bagginsdb/truetime`  
  Mock time service that provides timestamps for record inserts.

- `logger/`  
  A simple logging wrapper configured to produce structured logs via `zap`.

## Performance tuning and improvements
I've opted to track and log performance at quite a granular level. You can find saved graphs and performance notes in the [Performance](performance) folder

## Roadmap

- Add delete support (tombstones).
- Implement improved error handling and recovery after crashes.
- Integrate more benchmarks and profiling tools to guide optimisations.

## License

This project is distributed under the MIT License. See `LICENSE` for details.
