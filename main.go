package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type measurementStats struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

type fileChunk struct {
	start int
	end   int
}

type chunkGroup struct {
	start int
	end   int
}

func main() {
	inputPath := flag.String("input", "measurements.txt", "path to the measurements file")
	generate := flag.Bool("generate", false, "generate a measurements file instead of computing results")
	total := flag.Int("total", defaultTotalMeasurements, "number of rows to generate when using -generate")
	chunkSize := flag.Int("chunk", defaultChunkSize, "rows per chunk when generating data")
	workers := flag.Int("workers", runtime.NumCPU(), "number of worker goroutines when processing measurements")
	flag.Parse()

	if *workers > 0 && runtime.GOMAXPROCS(0) < *workers {
		runtime.GOMAXPROCS(*workers)
	}

	if *generate {
		timerDone := newRunTimer()
		if err := generateMeasurements(*inputPath, *total, *chunkSize); err != nil {
			log.Fatalf("generate failed: %v", err)
		}
		wall, cpu, haveCPU := timerDone()
		reportRunMetrics(fmt.Sprintf("generated %d measurements into %s", *total, *inputPath), wall, cpu, haveCPU)
		return
	}

	timerDone := newRunTimer()
	stats, err := calculate(*inputPath, *workers)
	if err != nil {
		log.Fatalf("processing failed: %v", err)
	}

	fmt.Println(formatStats(stats))
	wall, cpu, haveCPU := timerDone()
	reportRunMetrics("processing completed", wall, cpu, haveCPU)
}

func calculate(path string, workers int) (map[string]measurementStats, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		return make(map[string]measurementStats), nil
	}

	if size > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("file too large to map on this platform: %d bytes", size)
	}

	mapped, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	defer syscall.Munmap(mapped)

	if workers < 1 {
		workers = 1
	}
	if workers > len(mapped) && len(mapped) > 0 {
		workers = len(mapped)
	}

	chunkTarget := workers * 8
	chunks := splitIntoChunks(mapped, chunkTarget)
	if len(chunks) == 0 {
		return make(map[string]measurementStats), nil
	}

	groups := buildChunkGroups(chunks, workers)
	initialCount := workers
	if len(groups) < initialCount {
		initialCount = len(groups)
	}

	stealCount := 0
	if len(groups) > initialCount {
		stealCount = len(groups) - initialCount
	}
	stealCh := make(chan chunkGroup, stealCount)
	if stealCount > 0 {
		for _, grp := range groups[initialCount:] {
			stealCh <- grp
		}
	}
	close(stealCh)

	type chunkResult struct {
		data map[string]measurementStats
		err  error
	}

	results := make(chan chunkResult, workers)
	var wg sync.WaitGroup

	for workerID := 0; workerID < workers; workerID++ {
		wg.Add(1)
		var initial chunkGroup
		if workerID < len(groups) {
			initial = groups[workerID]
		}
		go func(initial chunkGroup) {
			defer wg.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			local := make(map[string]measurementStats, 512)
			if initial.end > initial.start {
				if err := processChunk(local, mapped, initial.start, initial.end); err != nil {
					results <- chunkResult{err: err}
					return
				}
			}

			for grp := range stealCh {
				if err := processChunk(local, mapped, grp.start, grp.end); err != nil {
					results <- chunkResult{err: err}
					return
				}
			}

			results <- chunkResult{data: local}
		}(initial)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	merged := make(map[string]measurementStats, 4096)
	var firstErr error

	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		if res.data != nil {
			mergeStats(merged, res.data)
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return merged, nil
}

func splitIntoChunks(data []byte, desired int) []fileChunk {
	size := len(data)
	if desired < 1 {
		desired = 1
	}
	if desired > size && size > 0 {
		desired = size
	}
	if desired == 0 {
		return nil
	}

	chunkSize := size / desired
	if chunkSize == 0 {
		chunkSize = 1
	}

	chunks := make([]fileChunk, 0, desired)
	start := 0
	for i := 0; i < desired && start < size; i++ {
		end := start + chunkSize
		if end > size {
			end = size
		}
		if end <= start {
			end = start + 1
			if end > size {
				end = size
			}
		}
		if end < size {
			for end < size && data[end-1] != '\n' {
				end++
			}
			if end > size {
				end = size
			}
		}
		chunks = append(chunks, fileChunk{start: start, end: end})
		start = end
	}

	if len(chunks) == 0 && size > 0 {
		return []fileChunk{{start: 0, end: size}}
	}

	return chunks
}

func buildChunkGroups(chunks []fileChunk, workers int) []chunkGroup {
	if len(chunks) == 0 {
		return nil
	}
	groups := make([]chunkGroup, 0, len(chunks))
	if workers < 1 {
		workers = 1
	}
	groupSize := (len(chunks) + workers - 1) / workers
	if groupSize < 1 {
		groupSize = 1
	}

	for i := 0; i < len(chunks); i += groupSize {
		endIndex := i + groupSize
		if endIndex > len(chunks) {
			endIndex = len(chunks)
		}
		start := chunks[i].start
		end := chunks[endIndex-1].end
		groups = append(groups, chunkGroup{start: start, end: end})
	}

	return groups
}

func processChunk(dst map[string]measurementStats, data []byte, start, end int) error {
	if end <= start {
		return nil
	}

	pos := start
	for pos < end {
		lineStart := pos
		for pos < end && data[pos] != '\n' {
			pos++
		}
		line := data[lineStart:pos]
		if pos < end {
			pos++
		}

		if len(line) == 0 {
			continue
		}

		sep := bytesIndexByte(line, ';')
		if sep == -1 {
			return fmt.Errorf("invalid line (missing ';'): %q", string(line))
		}

		station := string(line[:sep])
		tempValue, parseErr := parseTemperature(line[sep+1:])
		if parseErr != nil {
			return parseErr
		}

		stat := dst[station]
		if stat.count == 0 {
			dst[station] = measurementStats{
				min:   tempValue,
				max:   tempValue,
				sum:   int64(tempValue),
				count: 1,
			}
		} else {
			if tempValue < stat.min {
				stat.min = tempValue
			}
			if tempValue > stat.max {
				stat.max = tempValue
			}
			stat.sum += int64(tempValue)
			stat.count++
			dst[station] = stat
		}
	}

	return nil
}

func mergeStats(dst map[string]measurementStats, src map[string]measurementStats) {
	for station, stat := range src {
		existing, ok := dst[station]
		if !ok || existing.count == 0 {
			dst[station] = stat
			continue
		}
		if stat.min < existing.min {
			existing.min = stat.min
		}
		if stat.max > existing.max {
			existing.max = stat.max
		}
		existing.sum += stat.sum
		existing.count += stat.count
		dst[station] = existing
	}
}

func parseTemperature(value []byte) (int32, error) {
	if len(value) == 0 {
		return 0, errors.New("empty temperature value")
	}

	sign := int32(1)
	idx := 0
	if value[0] == '-' {
		sign = -1
		idx++
	} else if value[0] == '+' {
		idx++
	}

	var digitsAfter int
	var hasDecimal bool
	var accum int32
	for ; idx < len(value); idx++ {
		c := value[idx]
		if c == '.' {
			if hasDecimal {
				return 0, fmt.Errorf("invalid temperature: %q", string(value))
			}
			hasDecimal = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid temperature: %q", string(value))
		}
		accum = accum*10 + int32(c-'0')
		if hasDecimal {
			digitsAfter++
		}
	}

	if !hasDecimal {
		accum *= 10
	} else if digitsAfter == 0 {
		accum *= 10
	} else if digitsAfter > 1 {
		return 0, fmt.Errorf("invalid temperature precision: %q", string(value))
	}

	return sign * accum, nil
}

func formatStats(stats map[string]measurementStats) string {
	keys := make([]string, 0, len(stats))
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.Grow(len(stats) * 32)
	builder.WriteByte('{')
	for i, station := range keys {
		if i > 0 {
			builder.WriteString(", ")
		}
		stat := stats[station]
		builder.WriteString(station)
		builder.WriteByte('=')
		appendTemperature(&builder, stat.min)
		builder.WriteByte('/')
		appendTemperature(&builder, roundAverage(stat.sum, stat.count))
		builder.WriteByte('/')
		appendTemperature(&builder, stat.max)
	}
	builder.WriteByte('}')
	return builder.String()
}

func appendTemperature(builder *strings.Builder, value int32) {
	if value < 0 {
		builder.WriteByte('-')
		value = -value
	}
	integerPart := value / 10
	fraction := value % 10
	builder.WriteString(strconv.FormatInt(int64(integerPart), 10))
	builder.WriteByte('.')
	builder.WriteByte(byte('0' + fraction))
}

func roundAverage(sum int64, count int64) int32 {
	if count == 0 {
		return 0
	}
	rounded := math.Round(float64(sum) / float64(count))
	return int32(rounded)
}

func bytesIndexByte(data []byte, target byte) int {
	for i, b := range data {
		if b == target {
			return i
		}
	}
	return -1
}

func newRunTimer() func() (time.Duration, time.Duration, bool) {
	startWall := time.Now()
	startCPU, haveCPU := readProcessCPUTime()
	return func() (time.Duration, time.Duration, bool) {
		wall := time.Since(startWall)
		if !haveCPU {
			return wall, 0, false
		}
		endCPU, endOK := readProcessCPUTime()
		if !endOK {
			return wall, 0, false
		}
		return wall, endCPU - startCPU, true
	}
}

func readProcessCPUTime() (time.Duration, bool) {
	metricNames := []string{
		"/process/cpu:seconds",
		"/cpu/classes/total:cpu-seconds",
	}
	var samples [1]metrics.Sample
	for _, name := range metricNames {
		samples[0].Name = name
		metrics.Read(samples[:])
		val := samples[0].Value
		switch val.Kind() {
		case metrics.KindFloat64:
			secs := val.Float64()
			if math.IsNaN(secs) || math.IsInf(secs, 0) {
				continue
			}
			return time.Duration(secs * float64(time.Second)), true
		case metrics.KindUint64:
			secs := float64(val.Uint64())
			return time.Duration(secs * float64(time.Second)), true
		}
	}
	return 0, false
}

func reportRunMetrics(prefix string, wall time.Duration, cpu time.Duration, haveCPU bool) {
	if haveCPU {
		if wall > 0 {
			cpuPercent := float64(cpu) / float64(wall) * 100
			fmt.Fprintf(os.Stderr, "%s in %s (CPU %s, %.2f%% of wall time)\n", prefix, wall, cpu, cpuPercent)
			return
		}
		fmt.Fprintf(os.Stderr, "%s in %s (CPU %s)\n", prefix, wall, cpu)
		return
	}
	fmt.Fprintf(os.Stderr, "%s in %s\n", prefix, wall)
}
