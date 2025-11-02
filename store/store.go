package store

import (
	"hash/maphash"
	"sync"
	"time"
)

type entry struct {
	val      []byte
	expireAt int64
}

type shard struct {
	mu sync.RWMutex
	m  map[string]entry
}

type Config struct {
	Shards    int
	SweepEach time.Duration
}

type Store struct {
	shards []shard
	seed   maphash.Seed
	stop   chan struct{}
}

func New(cfg Config) *Store {
	if cfg.Shards <= 0 {
		cfg.Shards = 64
	}
	if cfg.SweepEach <= 0 {
		cfg.SweepEach = time.Second
	}
	s := &Store{
		shards: make([]shard, cfg.Shards),
		seed:   maphash.MakeSeed(),
		stop:   make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[string]entry, 1024)
	}
	go s.janitor(cfg.SweepEach)
	return s
}

func (s *Store) Close() { close(s.stop) }

func (s *Store) h(key string) int {
	var h maphash.Hash
	h.SetSeed(s.seed)
	h.WriteString(key)
	return int(h.Sum64() % uint64(len(s.shards)))
}

func (s *Store) Set(key string, val []byte, ttl time.Duration) {
	exp := int64(0)
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	sh := &s.shards[s.h(key)]
	sh.mu.Lock()
	sh.m[key] = entry{val: append([]byte(nil), val...), expireAt: exp}
	sh.mu.Unlock()
}

func (s *Store) Add(key string, val []byte, ttl time.Duration) bool {
	exp := int64(0)
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}
	sh := &s.shards[s.h(key)]
	now := time.Now().UnixNano()
	sh.mu.Lock()
	e, ok := sh.m[key]
	if ok && (e.expireAt == 0 || e.expireAt > now) {
		sh.mu.Unlock()
		return false
	}
	sh.m[key] = entry{val: append([]byte(nil), val...), expireAt: exp}
	sh.mu.Unlock()
	return true
}

func (s *Store) Get(key string) ([]byte, bool) {
	sh := &s.shards[s.h(key)]
	now := time.Now().UnixNano()
	sh.mu.RLock()
	e, ok := sh.m[key]
	if !ok {
		sh.mu.RUnlock()
		return nil, false
	}
	if e.expireAt != 0 && e.expireAt <= now {
		sh.mu.RUnlock()
		sh.mu.Lock()
		if e2, ok2 := sh.m[key]; ok2 && e2.expireAt != 0 && e2.expireAt <= now {
			delete(sh.m, key)
		}
		sh.mu.Unlock()
		return nil, false
	}
	v := append([]byte(nil), e.val...)
	sh.mu.RUnlock()
	return v, true
}

func (s *Store) Exists(key string) bool {
	sh := &s.shards[s.h(key)]
	now := time.Now().UnixNano()
	sh.mu.RLock()
	e, ok := sh.m[key]
	if !ok {
		sh.mu.RUnlock()
		return false
	}
	if e.expireAt != 0 && e.expireAt <= now {
		sh.mu.RUnlock()
		sh.mu.Lock()
		if e2, ok2 := sh.m[key]; ok2 && e2.expireAt != 0 && e2.expireAt <= now {
			delete(sh.m, key)
		}
		sh.mu.Unlock()
		return false
	}
	sh.mu.RUnlock()
	return true
}

func (s *Store) janitor(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now().UnixNano()
			for i := range s.shards {
				sh := &s.shards[i]
				sh.mu.Lock()
				n := 0
				for k, e := range sh.m {
					if e.expireAt != 0 && e.expireAt <= now {
						delete(sh.m, k)
					}
					n++
					if n > 1024 {
						break
					}
				}
				sh.mu.Unlock()
			}
		case <-s.stop:
			return
		}
	}
}
