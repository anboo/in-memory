package main

import (
	"flag"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"in-memory/client"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "server address")
	workers := flag.Int("workers", 32, "number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	flag.Parse()

	var totalOps int64
	var totalTime int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var latencies []int64
	var latMu sync.Mutex

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", id)
			val := []byte("payload-1234567890")

			c := client.New(*addr, 10)
			defer c.Close()

			for {
				select {
				case <-stop:
					return
				default:
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
			}
		}(i)
	}

	time.Sleep(*duration)
	close(stop)
	wg.Wait()

	ops := atomic.LoadInt64(&totalOps)
	avgUs := float64(totalTime) / float64(ops)
	rps := float64(ops) / duration.Seconds()

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

	fmt.Printf("\nAddress: %s\n", *addr)
	fmt.Printf("Workers: %d, Duration: %v\n", *workers, *duration)
	fmt.Printf("Total ops: %d\n", ops)
	fmt.Printf("Throughput: %.0f ops/sec\n", rps)
	fmt.Printf("Avg latency: %.2f µs (%.2f ms)\n", avgUs, avgUs/1000)
	fmt.Printf("p50=%d µs, p95=%d µs, p99=%d µs\n", p50, p95, p99)
}
