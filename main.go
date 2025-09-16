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
	"sort"
	"strconv"
	"strings"
)

type measurementStats struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

func main() {
	inputPath := flag.String("input", "measurements.txt", "path to the measurements file")
	generate := flag.Bool("generate", false, "generate a measurements file instead of computing results")
	total := flag.Int("total", defaultTotalMeasurements, "number of rows to generate when using -generate")
	chunkSize := flag.Int("chunk", defaultChunkSize, "rows per chunk when generating data")
	flag.Parse()

	if *generate {
		if err := generateMeasurements(*inputPath, *total, *chunkSize); err != nil {
			log.Fatalf("generate failed: %v", err)
		}
		fmt.Printf("generated %d measurements into %s\n", *total, *inputPath)
		return
	}

	stats, err := calculate(*inputPath)
	if err != nil {
		log.Fatalf("processing failed: %v", err)
	}

	fmt.Println(formatStats(stats))
}

func calculate(path string) (map[string]*measurementStats, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 1<<20)
	statsByStation := make(map[string]*measurementStats, 4096)

	var partial []byte
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			partial = append(partial, line...)
			continue
		}

		if len(partial) != 0 {
			line = append(partial, line...)
			partial = partial[:0]
		}

		if len(line) == 0 && errors.Is(err, io.EOF) {
			break
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

		stat := statsByStation[station]
		if stat == nil {
			statsByStation[station] = &measurementStats{
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
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	return statsByStation, nil
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

func formatStats(stats map[string]*measurementStats) string {
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
