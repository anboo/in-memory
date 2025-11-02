package main

import (
	"time"

	"in-memory/store"
)

func main() {
	st := store.New(store.Config{
		Shards:    1000,
		SweepEach: 5 * time.Second,
	})

	s := NewServer(":80", st)
	if err := s.Start(); err != nil {
		panic(err)
	}
}
