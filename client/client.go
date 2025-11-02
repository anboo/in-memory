package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"in-memory/generated/kv"

	"github.com/google/flatbuffers/go"
)

type Client struct {
	addr     string
	conn     net.Conn
	mu       sync.Mutex
	r        io.Reader
	backoff  time.Duration
	stopChan chan struct{}
	closed   bool
}

func New(addr string) *Client {
	c := &Client{
		addr:     addr,
		backoff:  time.Second,
		stopChan: make(chan struct{}),
	}
	go c.connectLoop()
	return c
}

// автоматическое переподключение с экспоненциальной задержкой
func (c *Client) connectLoop() {
	for {
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			time.Sleep(c.backoff)
			if c.backoff < 30*time.Second {
				c.backoff *= 2
			}
			continue
		}

		tcp := conn.(*net.TCPConn)
		tcp.SetNoDelay(true)

		c.mu.Lock()
		c.conn = conn
		c.r = conn
		c.backoff = time.Second
		c.mu.Unlock()

		fmt.Printf("[client] connected to %s\n", c.addr)
		return
	}
}

func (c *Client) reconnectAsync() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		conn.Close()
	}
	go c.connectLoop()
}

func (c *Client) Close() {
	c.mu.Lock()
	c.closed = true
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	close(c.stopChan)
}

var (
	builderPool = sync.Pool{
		New: func() any { return flatbuffers.NewBuilder(256) },
	}
	bufPool = sync.Pool{
		New: func() any { return make([]byte, 16*1024) },
	}
)

func (c *Client) send(req []byte) (*kv.Response, error) {
	c.mu.Lock()
	conn := c.conn
	r := c.r
	c.mu.Unlock()

	if conn == nil {
		return nil, errors.New("disconnected")
	}

	hdr := [4]byte{}
	binary.BigEndian.PutUint32(hdr[:], uint32(len(req)))

	c.mu.Lock()
	if _, err := conn.Write(hdr[:]); err != nil {
		c.mu.Unlock()
		c.reconnectAsync()
		return nil, err
	}
	if _, err := conn.Write(req); err != nil {
		c.mu.Unlock()
		c.reconnectAsync()
		return nil, err
	}
	c.mu.Unlock()

	respHdr := [4]byte{}
	if _, err := io.ReadFull(r, respHdr[:]); err != nil {
		c.reconnectAsync()
		return nil, err
	}
	size := binary.BigEndian.Uint32(respHdr[:])
	if size == 0 || size > 10_000_000 {
		return nil, fmt.Errorf("invalid resp size %d", size)
	}

	buf := bufPool.Get().([]byte)
	if cap(buf) < int(size) {
		buf = make([]byte, size)
	}
	body := buf[:size]
	if _, err := io.ReadFull(r, body); err != nil {
		bufPool.Put(buf)
		c.reconnectAsync()
		return nil, err
	}

	resp := kv.GetRootAsResponse(body, 0)
	bufPool.Put(buf)
	return resp, nil
}

// —————————————————————————
// helper для FlatBuffers-запросов
// —————————————————————————

func buildRequest(op kv.Op, key string, val []byte, ttl time.Duration) []byte {
	b := builderPool.Get().(*flatbuffers.Builder)
	b.Reset()

	keyOff := b.CreateString(key)
	var valOff flatbuffers.UOffsetT
	if len(val) > 0 {
		valOff = b.CreateByteVector(val)
	}

	kv.RequestStart(b)
	kv.RequestAddOp(b, op)
	kv.RequestAddKey(b, keyOff)
	if valOff != 0 {
		kv.RequestAddValue(b, valOff)
	}
	if ttl > 0 {
		kv.RequestAddTtlMs(b, uint64(ttl.Milliseconds()))
	}
	req := kv.RequestEnd(b)
	b.Finish(req)
	out := make([]byte, len(b.FinishedBytes()))
	copy(out, b.FinishedBytes())
	builderPool.Put(b)
	return out
}

// —————————————————————————
// операции
// —————————————————————————

func (c *Client) Set(key string, val []byte, ttl time.Duration) error {
	req := buildRequest(kv.OpSET, key, val, ttl)
	resp, err := c.send(req)
	if err != nil {
		return err
	}
	if !resp.Ok() {
		return fmt.Errorf("SET failed: %s", string(resp.Err()))
	}
	return nil
}

func (c *Client) Get(key string) ([]byte, error) {
	req := buildRequest(kv.OpGET, key, nil, 0)
	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}
	if !resp.Ok() {
		return nil, fmt.Errorf(string(resp.Err()))
	}
	return resp.ValueBytes(), nil
}

func (c *Client) Exists(key string) (bool, error) {
	req := buildRequest(kv.OpEXISTS, key, nil, 0)
	resp, err := c.send(req)
	if err != nil {
		return false, err
	}
	return resp.Exists(), nil
}

func (c *Client) Add(key string, val []byte, ttl time.Duration) (bool, error) {
	req := buildRequest(kv.OpADD, key, val, ttl)
	resp, err := c.send(req)
	if err != nil {
		return false, err
	}
	return resp.Ok(), nil
}
