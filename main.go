package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerInput struct {
	ID string
	Data interface{}
}

type Worker = func(WorkerInput)

const (
	WPS      = 10	// workers per second
	duration = 10*time.Second	// duration after ramp up
	rampUp   = 5 * time.Second	// warming up
)

var numWorkers int64
var wg sync.WaitGroup

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	w := func(i WorkerInput) {
		logger.Printf("Hello, this is worker %s\n", i.ID)
		time.Sleep(time.Second)
		//logger.Printf("Still working on %s\n", i.ID)
		time.Sleep(time.Second)
		logger.Printf("Phew, done %s\n", i.ID)
	}

	// Ramp up
	rampUpSecs := int(rampUp.Seconds())
	rampUpDelta := WPS / rampUpSecs

	for t := 1; t <= rampUpSecs; t++ {
		time.Sleep(time.Second)

		expectedNumWorkers := int64(t * rampUpDelta)
		actualNumWorkers := atomic.LoadInt64(&numWorkers)
		logger.Printf("actual #workers: %d, expected #workers: %d\n", actualNumWorkers, expectedNumWorkers)

		for j := actualNumWorkers; j < expectedNumWorkers; j++ {
			spawnWorker(w, WorkerInput{
				ID: fmt.Sprintf("%d", rand.Intn(100)),
			})
		}
	}
	logger.Printf("ramp up done, continuing with constant load\n")

	// Perf test
	for t := 0*time.Second; t < duration; t += time.Second {
		time.Sleep(time.Second)
		actualNumWorkers := atomic.LoadInt64(&numWorkers)
		logger.Printf("actual #workers: %d, expected #workers: %d\n", actualNumWorkers, WPS)

		for j := actualNumWorkers; j < WPS; j++ {
			spawnWorker(w, WorkerInput{
				ID: fmt.Sprintf("%d", rand.Intn(100)),
			})
		}
	}

	wg.Wait()

	logger.Printf("test finished, all workers done\n")
}

func spawnWorker(w Worker, i WorkerInput) {
	go func() {
		atomic.AddInt64(&numWorkers, 1)
		wg.Add(1)
		defer func() {
			atomic.AddInt64(&numWorkers, -1)
			wg.Done()
		}()

		w(i)
	}()
}