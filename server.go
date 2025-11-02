package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"in-memory/generated/kv"

	"github.com/google/flatbuffers/go"

	"in-memory/store"
)

var (
	bufPool = sync.Pool{
		New: func() any { return make([]byte, 64*1024) },
	}

	fbPool = sync.Pool{
		New: func() any { return flatbuffers.NewBuilder(256) },
	}
)

type Server struct {
	addr  string
	store *store.Store
}

func NewServer(addr string, st *store.Store) *Server {
	return &Server{addr: addr, store: st}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("[server] listening on %s", s.addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			err := tcpConn.SetNoDelay(true)
			if err != nil {
				fmt.Println("error setting tcp no delay:", err)
			}
			err = tcpConn.SetReadBuffer(64 * 1024)
			if err != nil {
				fmt.Println("error setting read buffer:", err)
			}
			err = tcpConn.SetWriteBuffer(64 * 1024)
			if err != nil {
				fmt.Println("error setting write buffer:", err)
			}
		}

		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		fmt.Println("connection closed:", conn.RemoteAddr())
	}()

	for {
		var hdr [4]byte
		if _, err := io.ReadFull(conn, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return // клиент сам закрыл
			}
			fmt.Println("read header error:", err)
			return
		}

		size := binary.BigEndian.Uint32(hdr[:])
		if size == 0 {
			continue
		}

		buf := bufPool.Get().([]byte)
		if cap(buf) < int(size) {
			buf = make([]byte, size)
		}
		body := buf[:size]

		if _, err := io.ReadFull(conn, body); err != nil {
			fmt.Println("read body error:", err)
			bufPool.Put(buf)
			return
		}

		req := kv.GetRootAsRequest(body, 0)
		respBytes := s.processAndBuild(req)

		totalLen := 4 + len(respBytes)
		if cap(buf) < totalLen {
			buf = make([]byte, totalLen)
		}
		out := buf[:totalLen]

		binary.BigEndian.PutUint32(out[:4], uint32(len(respBytes)))
		copy(out[4:], respBytes)

		if _, err := conn.Write(out); err != nil {
			fmt.Println("write error:", err)
			bufPool.Put(buf)
			return
		}

		bufPool.Put(buf)
	}
}

func (s *Server) processAndBuild(req *kv.Request) []byte {
	op := req.Op()

	// key в схеме — это string, но в Go-генераторе FlatBuffers он возвращается как []byte (без копии)
	key := string(req.Key()) // тут будет аллокация под string; ок для старта

	switch op {
	case kv.OpGET:
		val, ok := s.store.Get(key)
		if !ok {
			return buildResponse(false, false, nil, "not found")
		}
		return buildResponse(true, true, val, "")

	case kv.OpSET:
		ttl := time.Duration(req.TtlMs()) * time.Millisecond
		// value можно получить zero-copy как срез на буфер запроса:
		val := req.ValueBytes()
		// store.Set в твоей реализации копирует значение внутрь — это безопасно
		s.store.Set(key, val, ttl)
		return buildResponse(true, false, nil, "")

	case kv.OpEXISTS:
		ex := s.store.Exists(key)
		return buildResponse(true, ex, nil, "")

	case kv.OpADD:
		ttl := time.Duration(req.TtlMs()) * time.Millisecond
		val := req.ValueBytes()
		added := s.store.Add(key, val, ttl)
		// в ответе exists=true означает «ключ уже был»
		return buildResponse(added, !added, nil, "")

	default:
		return buildResponse(false, false, nil, "unknown op")
	}
}

func buildResponse(ok bool, exists bool, value []byte, errStr string) []byte {
	b := fbPool.Get().(*flatbuffers.Builder)
	b.Reset()

	var valOff, errOff flatbuffers.UOffsetT
	if len(value) > 0 {
		valOff = b.CreateByteVector(value)
	}
	if len(errStr) > 0 {
		errOff = b.CreateString(errStr)
	}

	kv.ResponseStart(b)
	kv.ResponseAddOk(b, ok)
	kv.ResponseAddExists(b, exists)
	if valOff != 0 {
		kv.ResponseAddValue(b, valOff)
	}
	if errOff != 0 {
		kv.ResponseAddErr(b, errOff)
	}
	resp := kv.ResponseEnd(b)
	b.Finish(resp)

	out := append([]byte(nil), b.FinishedBytes()...) // скопировать результат
	fbPool.Put(b)
	return out
}
