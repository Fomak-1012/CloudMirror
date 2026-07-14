package relay

import (
	"net"
	"sync"
)

// StreamMap 管理 TCP/UDP 流 ID 与 net.Conn 的双向映射。
// 每个流由一个递增的 uint16 ID 标识，用于在一根隧道上多路复用多个连接。
type StreamMap struct {
	mu     sync.Mutex
	nextID uint16            // 下一个可分配的流 ID
	conns  map[uint16]net.Conn // ID → 连接映射
}

// NewStreamMap 创建一个空的 StreamMap。
func NewStreamMap() *StreamMap {
	return &StreamMap{
		conns: make(map[uint16]net.Conn),
	}
}

// Add 将连接加入映射，分配一个新的自增 ID 并返回。
func (sm *StreamMap) Add(conn net.Conn) uint16 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	id := sm.nextID
	sm.nextID++
	sm.conns[id] = conn
	return id
}

// AddWithID 将连接以指定 ID 加入映射（覆盖已有条目）。
func (sm *StreamMap) AddWithID(conn net.Conn, id uint16) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.conns[id] = conn
}

// Get 根据流 ID 获取对应的连接，不存在则返回 nil。
func (sm *StreamMap) Get(id uint16) net.Conn {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.conns[id]
}

// Remove 从映射中删除指定流 ID 的条目。
func (sm *StreamMap) Remove(id uint16) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.conns, id)
}
