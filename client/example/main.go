package main

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"in-memory/client"
)

func main() {
	const (
		addr     = "10.173.147.170:80"
		poolSize = 32
		workers  = 32
		duration = 10 * time.Second
	)

	pool, _ := client.NewPool(addr, poolSize)
	defer pool.Close()

	var totalOps int64
	var totalTime int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	latencies := make([]int64, 0, 1_000_000)
	var latMu sync.Mutex

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			val := []byte("payload-1234567890")

			for {
				select {
				case <-stop:
					return
				default:
				}

				c, err := pool.Acquire()
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				t0 := time.Now()
				if err := c.Set(key, val, 0); err == nil {
					if _, err := c.Get(key); err == nil {
						dur := time.Since(t0).Microseconds()
						atomic.AddInt64(&totalOps, 2)
						atomic.AddInt64(&totalTime, dur)
						latMu.Lock()
						latencies = append(latencies, dur)
						latMu.Unlock()
					}
				}
				pool.Release(c)
			}
		}(i)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	ops := atomic.LoadInt64(&totalOps)
	avgUs := float64(totalTime) / float64(ops)
	rps := float64(ops) / duration.Seconds()

	// сортируем для процентилей
	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	latMu.Unlock()

	getPct := func(p float64) int64 {
		if n == 0 {
			return 0
		}
		idx := int(float64(n-1) * p)
		return latencies[idx]
	}

	p50 := getPct(0.50)
	p95 := getPct(0.95)
	p99 := getPct(0.99)

	fmt.Printf("\nPool size: %d, Workers: %d, Duration: %v\n", poolSize, workers, duration)
	fmt.Printf("Total ops: %d\n", ops)
	fmt.Printf("Throughput: %.0f ops/sec\n", rps)
	fmt.Printf("Avg latency: %.2f µs (%.2f ms)\n", avgUs, avgUs/1000)
	fmt.Printf("p50=%d µs, p95=%d µs, p99=%d µs\n", p50, p95, p99)
}
