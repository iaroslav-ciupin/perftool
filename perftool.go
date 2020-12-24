package perftool

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	WorkersPerSecond int64
	RampUpPeriod     time.Duration
	TestDuration     time.Duration
	Worker           Worker
	LogOutput        io.Writer
}

type PerfTool struct {
	wps          int64
	rampUp       time.Duration
	testDuration time.Duration

	worker     Worker
	numWorkers int64
	wg         sync.WaitGroup
	workerIDs  chan int64
	logger     *log.Logger
}

func New(cfg Config) (*PerfTool, error) {
	if cfg.Worker == nil {
		return nil, fmt.Errorf("worker func should not be nil")
	}
	if cfg.WorkersPerSecond == 0 {
		return nil, fmt.Errorf("workers per second should be positive integer")
	}
	if cfg.RampUpPeriod == 0 && cfg.TestDuration == 0 {
		return nil, fmt.Errorf("ramp up & test duration cannot be both zero")
	}
	logOutput := cfg.LogOutput
	if cfg.LogOutput == nil {
		logOutput = ioutil.Discard
	}
	return &PerfTool{
		wps:          cfg.WorkersPerSecond,
		rampUp:       cfg.RampUpPeriod,
		testDuration: cfg.TestDuration,
		worker:       cfg.Worker,

		logger: log.New(logOutput, "", log.LstdFlags),
	}, nil
}

type WorkerInput struct {
	ID int64
}

type Worker = func(WorkerInput)

// TODO make this configurable
const rampUpTick = 10 * time.Millisecond

func (p *PerfTool) Start() {
	p.workerIDs = make(chan int64, p.wps)
	for i := int64(0); i < p.wps; i++ {
		p.workerIDs <- i
	}

	rampUpSpeed := float64(p.wps) / p.rampUp.Seconds()
	p.logger.Printf("ramping up for %v increasing with speed %.2f worker/sec\n", p.rampUp, rampUpSpeed)
	p.spawnLoop(p.rampUp, func(t time.Duration) int64 {
		return int64(math.Ceil(rampUpSpeed * t.Seconds()))
	})
	p.logger.Println("ramp up done")

	p.logger.Printf("stress testing for %v with constant load %d workers/sec\n", p.testDuration, p.wps)
	p.spawnLoop(p.testDuration, func(_ time.Duration) int64 {
		return p.wps
	})
	p.wg.Wait()

	p.logger.Printf("test finished, all workers done\n")
}

func (p *PerfTool) spawnLoop(timeout time.Duration, expectedNumWorkers func(t time.Duration) int64) {
	startTime := time.Now()
	ticker := time.NewTicker(rampUpTick)
	for tNow := range ticker.C {
		t := tNow.Sub(startTime)
		if t > timeout {
			ticker.Stop()
			break
		}

		expectedNumWorkers := expectedNumWorkers(t)
		actualNumWorkers := atomic.LoadInt64(&p.numWorkers)
		if actualNumWorkers >= expectedNumWorkers {
			continue
		}

		p.logger.Printf("actual #workers: %d, expected #workers: %d\n", actualNumWorkers, expectedNumWorkers)
		for j := actualNumWorkers; j < expectedNumWorkers; j++ {
			p.spawnWorker()
		}
	}
}

func (p *PerfTool) spawnWorker() {
	atomic.AddInt64(&p.numWorkers, 1)
	p.wg.Add(1)
	go func() {
		id := <-p.workerIDs
		defer func() {
			p.workerIDs <- id
			atomic.AddInt64(&p.numWorkers, -1)
			p.wg.Done()
		}()

		p.worker(WorkerInput{ID: id})
	}()
}
