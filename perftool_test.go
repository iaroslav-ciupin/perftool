package perftool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testWriter struct {
	t *testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}

func Test_Basic(t *testing.T) {
	tWriter := testWriter{t: t}
	cfg := Config{
		WorkersPerSecond: 10,
		RampUpPeriod:     5 * time.Second,
		TestDuration:     10 * time.Second,
		Worker: func(i WorkerInput) {
			t.Logf("Hello, this is worker %d\n", i.ID)
			time.Sleep(4 * time.Second)
			t.Logf("Phew, done %d\n", i.ID)
		},
		LogOutput: tWriter,
	}
	tool, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, tool)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tool.Start()
	}()

	time.Sleep(cfg.RampUpPeriod)

	for i := 0; i < 10; i++ {
		assert.InDelta(t, cfg.WorkersPerSecond, atomic.LoadInt64(&tool.numWorkers), 1.0)
		time.Sleep(time.Second)
	}
}

func Test_ManyWorkers(t *testing.T) {
	hit := make([]bool, 1000)
	cfg := Config{
		WorkersPerSecond: 1000,
		RampUpPeriod:     10 * time.Second,
		TestDuration:     10 * time.Second,
		Worker: func(i WorkerInput) {
			if i.ID < 0 || i.ID >= 1000 {
				t.Fatalf("malformed worker id %d", i.ID)
			}
			hit[i.ID] = true
			time.Sleep(1 * time.Second)
		},
	}
	tool, err := New(cfg)
	assert.NoError(t, err)
	assert.NotNil(t, tool)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tool.Start()
	}()

	time.Sleep(cfg.RampUpPeriod)

	for i := 0; i < 10; i++ {
		assert.InDelta(t, cfg.WorkersPerSecond, atomic.LoadInt64(&tool.numWorkers), 50.0)
		time.Sleep(time.Second)
	}
	for i := 0; i < 1000; i++ {
		assert.True(t, hit[i])
	}
}
