package client

import (
	"errors"
	"sync"
)

type Pool struct {
	addr   string
	size   int
	mu     sync.Mutex
	conns  chan *Client
	closed bool
}

func NewPool(addr string, size int) (*Pool, error) {
	if size <= 0 {
		size = 1
	}
	p := &Pool{
		addr:  addr,
		size:  size,
		conns: make(chan *Client, size),
	}

	for i := 0; i < size; i++ {
		c := New(addr, size)
		p.conns <- c
	}

	return p, nil
}

func (p *Pool) Acquire() (*Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("pool closed")
	}
	p.mu.Unlock()

	c, ok := <-p.conns
	if !ok {
		return nil, errors.New("pool closed")
	}

	return c, nil
}

func (p *Pool) Release(c *Client) {
	if c == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		c.Close()
		return
	}
	select {
	case p.conns <- c:
	default:
		c.Close()
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.conns)
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
}
