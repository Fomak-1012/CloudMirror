package relay

import (
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// PeerMap 管理 listener 与 forwarder 的配对关系。
// 按 index 分组，一个 listener 可以对应多个 forwarder（广播模式）。
type PeerMap struct {
	mu         sync.Mutex
	listeners  map[int]protocol.FrameReadWriter      // index → listener 连接
	forwarders map[int][]protocol.FrameReadWriter    // index → forwarder 连接列表
}

// NewPeerMap 创建一个空的 PeerMap。
func NewPeerMap() *PeerMap {
	return &PeerMap{
		listeners:  make(map[int]protocol.FrameReadWriter),
		forwarders: make(map[int][]protocol.FrameReadWriter),
	}
}

// RegisterListener 在指定 index 注册一个 listener，替换旧的（若存在）。
func (pm *PeerMap) RegisterListener(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if old, ok := pm.listeners[index]; ok {
		old.Close()
	}
	pm.listeners[index] = conn
}

// GetListener 返回指定 index 的 listener 连接（可能为 nil）。
// 用于查询 listener 的元信息（如模式），不持锁返回。
func (pm *PeerMap) GetListener(index int) protocol.FrameReadWriter {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.listeners[index]
}

// RegisterForwarder 向指定 index 追加一个 forwarder。
// 允许多个 forwarder 注册到同一个 index，实现广播。
func (pm *PeerMap) RegisterForwarder(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.forwarders[index] = append(pm.forwarders[index], conn)
}

// UnregisterListener 移除 listener 并通知该 index 下所有 forwarder
// 对端已断开（PeerLeave），然后清空 forwarder 列表。
func (pm *PeerMap) UnregisterListener(index int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.listeners, index)
	for _, fwd := range pm.forwarders[index] {
		fwd.Send(protocol.TypePeerLeave, nil)
	}
	delete(pm.forwarders, index)
}

// UnregisterForwarder 从指定 index 的 forwarder 列表中移除特定的连接。
// 若该 index 下已无 forwarder，通知 listener 所有 forwarder 已离开。
func (pm *PeerMap) UnregisterForwarder(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	fwdList := pm.forwarders[index]
	for i, f := range fwdList {
		if f == conn {
			pm.forwarders[index] = append(fwdList[:i], fwdList[i+1:]...)
			break
		}
	}
	if len(pm.forwarders[index]) == 0 {
		if lis, ok := pm.listeners[index]; ok {
			lis.Send(protocol.TypePeerLeave, nil)
		}
	}
}

// BroadcastFromListener 将 listener 发来的帧广播给所有 forwarder，
// 同时过滤掉已断开的 forwarder 连接。
func (pm *PeerMap) BroadcastFromListener(index int, typ byte, payload []byte) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	peers := pm.forwarders[index]
	alive := peers[:0]
	for _, p := range peers {
		if p.Send(typ, payload) == nil {
			alive = append(alive, p)
		}
	}
	if len(alive) != len(peers) {
		pm.forwarders[index] = alive
	}
}

// SendToListener 将 forwarder 发来的帧转发给该 index 的 listener。
func (pm *PeerMap) SendToListener(index int, typ byte, payload []byte) error {
	pm.mu.Lock()
	lis := pm.listeners[index]
	pm.mu.Unlock()
	if lis == nil {
		return nil
	}
	return lis.Send(typ, payload)
}

// ListenerCount 返回当前活跃的 listener 数量。
func (pm *PeerMap) ListenerCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.listeners)
}

// HasListener 检查指定 index 是否存在 listener。
func (pm *PeerMap) HasListener(index int) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	_, ok := pm.listeners[index]
	return ok
}

// ForwarderCount 返回指定 index 下的 forwarder 数量。
func (pm *PeerMap) ForwarderCount(index int) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.forwarders[index])
}

// ListenerIndices 返回所有 listener index 的快照。
// 用于 forwarder 自动配对（仅一个 listener 时可省略 -i 参数）。
func (pm *PeerMap) ListenerIndices() []int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if len(pm.listeners) == 0 {
		return nil
	}
	indices := make([]int, 0, len(pm.listeners))
	for i := range pm.listeners {
		indices = append(indices, i)
	}
	return indices
}
