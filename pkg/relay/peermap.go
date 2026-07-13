package relay

import (
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// PeerMap manages the pairing of listeners and forwarders by index.
// Multiple forwarders can be registered at the same index (broadcast mode).
type PeerMap struct {
	mu         sync.Mutex
	listeners  map[int]protocol.FrameReadWriter
	forwarders map[int][]protocol.FrameReadWriter
}

// NewPeerMap creates an empty PeerMap.
func NewPeerMap() *PeerMap {
	return &PeerMap{
		listeners:  make(map[int]protocol.FrameReadWriter),
		forwarders: make(map[int][]protocol.FrameReadWriter),
	}
}

// RegisterListener adds a listener at the given index, replacing any stale one.
func (pm *PeerMap) RegisterListener(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if old, ok := pm.listeners[index]; ok {
		old.Close()
	}
	pm.listeners[index] = conn
}

// RegisterForwarder appends a forwarder at the given index.
func (pm *PeerMap) RegisterForwarder(index int, conn protocol.FrameReadWriter) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.forwarders[index] = append(pm.forwarders[index], conn)
}

// UnregisterListener removes a listener and notifies all forwarders at
// the same index with a PeerLeave frame.
func (pm *PeerMap) UnregisterListener(index int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.listeners, index)
	for _, fwd := range pm.forwarders[index] {
		fwd.Send(protocol.TypePeerLeave, nil)
	}
	delete(pm.forwarders, index)
}

// UnregisterForwarder removes a specific forwarder from the slice.
// It notifies the listener only when no forwarders remain at the index.
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

// BroadcastFromListener sends a frame to all forwarders at the given index,
// removing any dead connections from the slice.
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

// SendToListener sends a frame from a forwarder to the listener at the
// given index. Returns the listener's Send error (or nil if no listener).
func (pm *PeerMap) SendToListener(index int, typ byte, payload []byte) error {
	pm.mu.Lock()
	lis := pm.listeners[index]
	pm.mu.Unlock()
	if lis == nil {
		return nil
	}
	return lis.Send(typ, payload)
}

// ListenerCount returns the number of active listeners.
func (pm *PeerMap) ListenerCount() int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.listeners)
}

// HasListener returns whether a listener exists at the given index.
func (pm *PeerMap) HasListener(index int) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	_, ok := pm.listeners[index]
	return ok
}

// ForwarderCount returns the number of forwarders at the given index.
func (pm *PeerMap) ForwarderCount(index int) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return len(pm.forwarders[index])
}

// ListenerIndices returns a snapshot of all listener indices.
// Returns nil for the single-listener case to allow auto-pairing.
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
