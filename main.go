package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type measurementStats struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

type fileChunk struct {
	start int64
	end   int64
}

func main() {
	inputPath := flag.String("input", "measurements.txt", "path to the measurements file")
	generate := flag.Bool("generate", false, "generate a measurements file instead of computing results")
	total := flag.Int("total", defaultTotalMeasurements, "number of rows to generate when using -generate")
	chunkSize := flag.Int("chunk", defaultChunkSize, "rows per chunk when generating data")
	workers := flag.Int("workers", runtime.NumCPU(), "number of worker goroutines when processing measurements")
	flag.Parse()

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

	if workers < 1 {
		workers = 1
	}
	if int64(workers) > size {
		workers = int(size)
		if workers == 0 {
			workers = 1
		}
	}

	chunks, err := splitIntoChunks(file, size, workers)
	if err != nil {
		return nil, err
	}

	type chunkResult struct {
		data map[string]measurementStats
		err  error
	}

	results := make(chan chunkResult, len(chunks))
	var wg sync.WaitGroup
	for _, chunk := range chunks {
		wg.Add(1)
		go func(c fileChunk) {
			defer wg.Done()
			data, err := processChunk(file, c.start, c.end)
			results <- chunkResult{data: data, err: err}
		}(chunk)
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
		mergeStats(merged, res.data)
	}

	if firstErr != nil {
		return nil, firstErr
	}

	return merged, nil
}

func splitIntoChunks(file *os.File, size int64, workers int) ([]fileChunk, error) {
	if workers < 1 {
		workers = 1
	}
	chunkSize := size / int64(workers)
	if chunkSize == 0 {
		chunkSize = size
	}

	chunks := make([]fileChunk, 0, workers)
	var start int64
	for i := 0; i < workers && start < size; i++ {
		end := start + chunkSize
		if i == workers-1 || end >= size {
			end = size
		} else {
			var err error
			end, err = advanceToNextNewline(file, end, size)
			if err != nil {
				return nil, err
			}
			if end > size {
				end = size
			}
		}
		if end < start {
			end = start
		}
		chunks = append(chunks, fileChunk{start: start, end: end})
		start = end
	}

	if len(chunks) == 0 && size > 0 {
		return []fileChunk{{start: 0, end: size}}, nil
	}

	return chunks, nil
}

func advanceToNextNewline(file *os.File, offset int64, size int64) (int64, error) {
	if offset >= size {
		return size, nil
	}

	buf := make([]byte, 64*1024)
	current := offset
	for {
		if current >= size {
			return size, nil
		}
		toRead := int64(len(buf))
		if remaining := size - current; remaining < toRead {
			toRead = remaining
		}
		n, err := file.ReadAt(buf[:toRead], current)
		if n == 0 {
			if err == io.EOF {
				return size, nil
			}
			return size, err
		}
		for i := 0; i < n; i++ {
			if buf[i] == '\n' {
				return current + int64(i+1), nil
			}
		}
		current += int64(n)
		if err == io.EOF {
			return size, nil
		}
	}
}

func processChunk(file *os.File, start, end int64) (map[string]measurementStats, error) {
	if end <= start {
		return make(map[string]measurementStats), nil
	}

	reader := io.NewSectionReader(file, start, end-start)
	bufReader := bufio.NewReaderSize(reader, 1<<20)
	stats := make(map[string]measurementStats, 512)
	var partial []byte

	for {
		line, err := bufReader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			partial = append(partial, line...)
			continue
		}

		if len(partial) != 0 {
			line = append(partial, line...)
			partial = partial[:0]
		}

		if len(line) == 0 {
			if err == nil {
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, err
			}
		}

		if len(line) > 0 && line[len(line)-1] == '\n' {
			line = line[:len(line)-1]
		}

		if len(line) == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			continue
		}

		sep := bytesIndexByte(line, ';')
		if sep == -1 {
			return nil, fmt.Errorf("invalid line (missing ';'): %q", string(line))
		}

		station := string(line[:sep])
		tempValue, parseErr := parseTemperature(line[sep+1:])
		if parseErr != nil {
			return nil, parseErr
		}

		stat := stats[station]
		if stat.count == 0 {
			stats[station] = measurementStats{
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
			stats[station] = stat
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return stats, nil
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
