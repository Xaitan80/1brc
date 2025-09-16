# 1BRC Go Solution

This repository contains a Go implementation for the [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc). It includes a high-throughput processor for the `measurements.txt` dataset and a parallel data generator for creating large synthetic files.

## Requirements

- Go 1.24+
- macOS (tested on Apple Silicon). The solution uses `syscall.Mmap`, which is available on Unix-like systems.

## Building

```bash
go build ./...
```

If your environment blocks the default Go build cache (as in some sandboxes), set a local cache location:

```bash
GOCACHE=$(pwd)/.gocache go build ./...
```

## Usage

The binary exposes CLI flags for both processing and generation.

### Processing an Existing Dataset

```bash
go run . -input measurements.txt
```

Key flags:

- `-input` (string, default `measurements.txt`): Path to the dataset to aggregate.
- `-workers` (int, default `runtime.NumCPU()`): Number of worker goroutines to use. The program sets `GOMAXPROCS` up to this value to keep all cores busy. Increase to saturate CPU, reduce if you are memory or I/O constrained.

Example with explicit worker count:

```bash
go run . -input measurements.txt -workers 8
```

The program prints the `{station=min/avg/max, ...}` map to stdout and reports wall-clock/CPU time to stderr.

### Generating Synthetic Data

```bash
go run . -generate -input measurements.txt
```

Generation flags:

- `-generate` (bool): Switch to generation mode.
- `-input` (string): Output path for the generated file.
- `-total` (int, default 1,000,000,000): Number of records to produce. Adjust to fit disk capacity.
- `-chunk` (int, default 100,000): Number of records per chunk when generating. Larger chunks reduce scheduling overhead, smaller ones reduce peak memory usage.

The generator writes measurements using a set of built-in station names. It streams data to disk using buffered writers and a pool of goroutines.

## Performance Notes

- Processing uses `mmap` to avoid extra copies and manually splits the file into newline-aligned chunks that run in parallel.
- Temperatures are parsed into 0.1°C units stored as integers to avoid floating-point drift and ensure one decimal place in the final output.
- Aggregation merges per-chunk stats maps after workers finish.

## Tips

- On systems with limited permissions to `/Library/Caches/go-build`, use a local `GOCACHE` as shown above.
- Experiment with `-workers` to find the best throughput for your machine; on an M2 Air, running near the physical core count works well.
- For correctness validation, you can generate a smaller test file and compare results to a reference implementation.

