package main

import (
	"bufio"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const (
	defaultTotalMeasurements = 1_000_000_000
	defaultChunkSize         = 100_000
)

var stations = []string{"Stockholm", "Berlin", "Madrid", "Paris", "Oslo"}

func generateMeasurements(path string, total int, chunkSize int) (err error) {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	if total <= 0 {
		total = defaultTotalMeasurements
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 1<<20)
	defer func() {
		if flushErr := writer.Flush(); err == nil && flushErr != nil {
			err = flushErr
		}
	}()

	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}
	jobs := make(chan int, workerCount)
	chunks := make(chan []byte, workerCount*2)

	var wg sync.WaitGroup
	for workerID := 0; workerID < workerCount; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			localStations := stations

			for chunkIndex := range jobs {
				start := chunkIndex * chunkSize
				remaining := total - start
				count := chunkSize
				if remaining < chunkSize {
					count = remaining
				}
				if count <= 0 {
					continue
				}

				buf := make([]byte, 0, count*24)
				for i := 0; i < count; i++ {
					station := localStations[rnd.Intn(len(localStations))]
					buf = append(buf, station...)
					buf = append(buf, ';')
					temp := -20 + rnd.Float64()*60
					buf = strconv.AppendFloat(buf, temp, 'f', 1, 64)
					buf = append(buf, '\n')
				}

				chunks <- buf
			}
		}(workerID)
	}

	go func() {
		wg.Wait()
		close(chunks)
	}()

	totalChunks := (total + chunkSize - 1) / chunkSize
	go func() {
		for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
			jobs <- chunkIndex
		}
		close(jobs)
	}()

	for chunk := range chunks {
		if _, err = writer.Write(chunk); err != nil {
			return err
		}
	}

	return nil
}
