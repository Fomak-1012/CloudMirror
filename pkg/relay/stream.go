package relay

import (
	"net"
	"sync"
)

type StreamMap struct {
	mu     sync.Mutex
	nextId uint16
	conns  map[uint16]net.Conn
}

func NewStreamMap() *StreamMap {
	return &StreamMap{
		conns: make(map[uint16]net.Conn),
	}
}

func (sm *StreamMap) Add(conn net.Conn) uint16 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := sm.nextId
	sm.nextId++
	sm.conns[id] = conn
	return id
}

func (sm *StreamMap) AddWithId(conn net.Conn, id uint16) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.conns[id] = conn
}

func (sm *StreamMap) Get(id uint16) net.Conn {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.conns[id]
}

func (sm *StreamMap) Remove(id uint16) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.conns, id)
}
