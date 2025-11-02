package main

import (
	"flag"
	"fmt"
	"time"

	"in-memory/store"
)

func main() {
	addr := flag.String("addr", ":9001", "listen address, e.g. :9001")
	shards := flag.Int("shards", 1000, "number of shards in memory store")
	sweep := flag.Duration("sweep", 5*time.Second, "TTL sweep interval")
	flag.Parse()

	st := store.New(store.Config{
		Shards:    *shards,
		SweepEach: *sweep,
	})

	fmt.Printf("[server] starting on %s, shards=%d, sweep=%v\n", *addr, *shards, *sweep)
	s := NewServer(*addr, st)

	if err := s.Start(); err != nil {
		panic(err)
	}
}
