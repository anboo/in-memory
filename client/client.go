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
	reqQueue chan *pipelineReq
	pending  chan *pipelineReq // очередь ожидающих ответов
	backoff  time.Duration
	stopChan chan struct{}
}

type pipelineReq struct {
	data []byte
	resp chan *kv.Response
	err  chan error
}

func New(addr string, pipelineDepth int) *Client {
	c := &Client{
		addr:     addr,
		backoff:  time.Second,
		reqQueue: make(chan *pipelineReq, pipelineDepth),
		pending:  make(chan *pipelineReq, pipelineDepth),
		stopChan: make(chan struct{}),
	}
	go c.connectLoop()
	return c
}

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
		c.backoff = time.Second
		c.mu.Unlock()

		fmt.Printf("[client] connected to %s\n", c.addr)

		go c.writer()
		go c.reader()
		return
	}
}

func (c *Client) writer() {
	for r := range c.reqQueue {
		hdr := [4]byte{}
		binary.BigEndian.PutUint32(hdr[:], uint32(len(r.data)))
		if _, err := c.conn.Write(hdr[:]); err != nil {
			r.err <- err
			continue
		}
		if _, err := c.conn.Write(r.data); err != nil {
			r.err <- err
			continue
		}
		c.pending <- r // отправили — теперь ждём ответ
	}
}

func (c *Client) reader() {
	defer c.conn.Close()
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				fmt.Println("[reader] closed:", err)
			} else {
				fmt.Println("[reader] read header err:", err)
			}
			return
		}
		size := binary.BigEndian.Uint32(hdr[:])
		if size == 0 || size > 10_000_000 {
			continue
		}

		body := make([]byte, size)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			fmt.Println("[reader] read body err:", err)
			return
		}
		resp := kv.GetRootAsResponse(body, 0)

		select {
		case req := <-c.pending:
			req.resp <- resp
		default:
			// если что-то рассинхронизировалось
			fmt.Println("[reader] warning: response without pending request")
		}
	}
}

func (c *Client) Close() {
	close(c.stopChan)
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock()
}

func (c *Client) sendPipeline(req []byte) (*kv.Response, error) {
	p := &pipelineReq{
		data: req,
		resp: make(chan *kv.Response, 1),
		err:  make(chan error, 1),
	}
	select {
	case c.reqQueue <- p:
	default:
		return nil, errors.New("pipeline full")
	}
	select {
	case resp := <-p.resp:
		return resp, nil
	case err := <-p.err:
		return nil, err
	}
}

var builderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(256) },
}

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

// операции
func (c *Client) Set(key string, val []byte, ttl time.Duration) error {
	req := buildRequest(kv.OpSET, key, val, ttl)
	resp, err := c.sendPipeline(req)
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
	resp, err := c.sendPipeline(req)
	if err != nil {
		return nil, err
	}
	if !resp.Ok() {
		return nil, fmt.Errorf(string(resp.Err()))
	}
	return resp.ValueBytes(), nil
}
