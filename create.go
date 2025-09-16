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
	totalMeasurements = 1_000_000_000
	chunkSize         = 100_000
)

var stations = []string{"Stockholm", "Berlin", "Madrid", "Paris", "Oslo"}

func main() {
	file, err := os.Create("measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, 1<<20) // keep disk writes large
	defer func() {
		if err := writer.Flush(); err != nil {
			panic(err)
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
				remaining := totalMeasurements - start
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
					temp := -20 + rnd.Float64()*60 // mellan -20 och +40 grader
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

	totalChunks := (totalMeasurements + chunkSize - 1) / chunkSize
	for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
		jobs <- chunkIndex
	}
	close(jobs)

	for chunk := range chunks {
		if _, err := writer.Write(chunk); err != nil {
			panic(err)
		}
	}
}
