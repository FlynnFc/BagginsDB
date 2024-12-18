# Here outlines the performance steps taken throughout the project

_Fuzz testing workload.
This consists of random length data, invalid data, and a changeable mix of reads and writes._

_Each test runs 5 times and the final result is an average of the runs_

## Version 0.1.0 - Baseline

### Bulk Writes

- 10000 operations in 38.8192ms (257604.48 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 848.6951ms (23565.59 ops/sec)

### Fuzz Testing Workload

- 30% writes 27,725 ops/sec

- 80% writes 138,221 ops/sec

### Takeaways

Read performance is completely awful. Can implement sstable offset indexing + potentially Read/Write locks or lock free reads?

## Version 0.2.0 - Improvement of ~350% overall

### Bulk Writes

- 10000 operations in 38.8192ms (167,002.06 ops/sec)

### Bulk Reads

- 10000 operations in 13.0777ms (764,660.45 ops/sec)

### Mixed Workload 20% writes

- 20000 operations in 848.6951ms (135,541.85 ops/sec)

### Fuz Testing Workload

- 30% writes (200,836 ops/sec)
