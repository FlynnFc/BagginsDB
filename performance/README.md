# Here outlines the performance steps taken throughout the project

_Fuzz testing workload.
This consists of random length data, invalid data, and a changeable mix of reads and writes._

_Each test runs 5 times and the final result is an average of the runs_

_ops/sec are on a single node unless specified otherwise_

## Version 0.1.0 - Baseline

### Bulk Writes

- 10000 operations in 38.8192ms (257604.48 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 848.6951ms (23565.59 ops/sec)

### Fuzz Testing Workload

- 30% writes 27,725 ops/sec

- 80% writes 138,221 ops/sec

### Takeaways

The read performance is completely awful. Can implement sstable offset indexing + potentially Read/Write locks or lock-free reads?

## Version 0.1.1

The main change here is a more efficient compaction

### Bulk Writes

- 10000 operations in 38.8192ms (167,002.06 ops/sec)

### Bulk Reads

- 10000 operations in 13.0777ms (110,660.45 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 848.6951ms (135,541.85 ops/sec)

### Fuz Testing Workload

- 30% writes (200,836 ops/sec)

## Version 0.1.2

Core changes here were putting compaction in a worker and increasing memtable size from 1kb to 1mb

### Bulk Writes

- 1,000,000 operations in 3.2542362s (307,291.77 ops/sec)

### Bulk Reads

- 1,000,000 operations in 3.1618652s (316,269.02 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 55.6723ms (359,245.08 ops/sec)

### Fuz Testing Workload

- 30% writes (347,734 ops/sec)

### Takeaways

Looking at the function call graph, the third-party skiplist is now a huge bottleneck (pprof003) as well as my trueTime implementation. Would benefit from implementing our own/ exploring other possible memtable data structures

## Version 0.1.3

Core changes here were optimising compaction and the bloom filter as well as increasing memtable size from 1kb to 1mb

### Bulk Writes

- No change

### Bulk Reads

- No change

### Mixed Workload 20% writes

- No change

### Fuz Testing Workload

- 30% writes (594,131 ops/sec)

### Takeaways

After implementing my own thread safe skiplist we got a large improvement for simulation and fuz testing. No meaningful change on bulk operations. But this could be hidden by the fact there is more locking happening now.

## Version 0.2.0

Pivoted from key-value store to wide column store. Improved locking granularity. Improved skiplist.

### Bulk Writes

- 1,000,000 operations in 3.7891508s (263,911.38 ops/sec)

### Bulk Reads

- 1,000,000 operations in 522.8734ms (1,912,508.84 ops/sec)

### Non-existent Keys Retrieval

- 1,000 operations in 999.6µs (1,000,400.16 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 55.6723ms (629,588.12 ops/sec)

### Fuz Testing Workload

- 30% writes (760,784 ops/sec)

### Takeaways

Getting slowly better at managing locks and file read/writes. Bulk reads have gotten quick but unrealistic in prod environments!
