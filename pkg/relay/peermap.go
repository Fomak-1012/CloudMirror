package relay

import (
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// listener 与 forwarder 的配对关系
// 按 index 分组，一个 listener 可以对应多个 forwarder
type PeerMap struct {
	mu         sync.Mutex
	listeners  map[int]protocol.FrameReadWriter
	forwarders map[int][]protocol.FrameReadWriter // forwarder 连接列表
}

func NewPeerMap() *PeerMap {
	return &PeerMap{
		listeners:  make(map[int]protocol.FrameReadWriter),
		forwarders: make(map[int][]protocol.FrameReadWriter),
	}
}

// 在指定 index 注册一个 listener
func (pm *PeerMap) RegisterListener(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if old, ok := pm.listeners[index]; ok {
		old.Close()
	}
	pm.listeners[index] = conn
}

// 返回指定 index 的 listener 连接
// 仅查询，不持锁
func (pm *PeerMap) GetListener(index int) protocol.FrameReadWriter {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.listeners[index]
}

// 向指定 index 加一个 forwarder
func (pm *PeerMap) RegisterForwarder(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.forwarders[index] = append(pm.forwarders[index], conn)
}

// 移除 listener
func (pm *PeerMap) UnregisterListener(index int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.listeners, index)

	//通知该 index 下所有 forwarder
	for _, fwd := range pm.forwarders[index] {
		fwd.Send(protocol.TypePeerLeave, nil)
	}
	// 清空 forwarder 列表
	delete(pm.forwarders, index)
}

// 从指定 index 的 forwarder 列表中移除特定的连接
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

	// 若该 index 下已无 forwarder，通知 listener 所有 forwarder 已离开
	if len(pm.forwarders[index]) == 0 {
		if lis, ok := pm.listeners[index]; ok {
			lis.Send(protocol.TypePeerLeave, nil)
		}
	}
}

// 将 listener 发来的帧广播给所有 forwarder
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

// 将 forwarder 发来的帧转发给该 index 的 listener
func (pm *PeerMap) SendToListener(index int, typ byte, payload []byte) error {
	pm.mu.Lock()
	lis := pm.listeners[index]
	pm.mu.Unlock()
	if lis == nil {
		return nil
	}
	return lis.Send(typ, payload)
}

// 返回当前活跃的 listener 数量
func (pm *PeerMap) ListenerCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.listeners)
}

// 检查指定 index 是否存在 listener
func (pm *PeerMap) HasListener(index int) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	_, ok := pm.listeners[index]
	return ok
}

// 返回指定 index 下的 forwarder 数量
func (pm *PeerMap) ForwarderCount(index int) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.forwarders[index])
}

// 返回所有 listener index 的快照
// 用于 forwarder 自动配对
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

// 持锁遍历所有 listener，对每个条目调用 fn
func (pm *PeerMap) RangeListeners(fn func(index int, conn protocol.FrameReadWriter)) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for idx, conn := range pm.listeners {
		fn(idx, conn)
	}
}

// 持锁遍历所有 forwarder，对每个条目调用 fn
func (pm *PeerMap) RangeForwarders(fn func(index int, conn protocol.FrameReadWriter)) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for idx, fwdList := range pm.forwarders {
		for _, conn := range fwdList {
			fn(idx, conn)
		}
	}
}

// 返回所有 index 下 forwarder 的总数
func (pm *PeerMap) ForwarderTotal() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	total := 0
	for _, fwdList := range pm.forwarders {
		total += len(fwdList)
	}
	return total
}
