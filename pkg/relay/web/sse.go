package web

import (
	"sync"
)

// sseHub 管理 SSE 订阅者，peers 变更时广播给所有客户端。
type sseHub struct {
	mu          sync.Mutex
	subscribers map[chan []byte]struct{}
}

func newSSEHub() *sseHub {
	return &sseHub{
		subscribers: make(map[chan []byte]struct{}),
	}
}

// subscribe 返回一个接收广播的通道。调用方负责在断开时调用 unsubscribe。
func (h *sseHub) subscribe() chan []byte {
	ch := make(chan []byte, 1)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

// unsubscribe 移除订阅者并关闭通道。
func (h *sseHub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.subscribers, ch)
	h.mu.Unlock()
}

// broadcast 向所有订阅者发送数据，不阻塞。
func (h *sseHub) broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subscribers {
		select {
		case ch <- data:
		default: // 订阅者消费太慢，跳过
		}
	}
}
