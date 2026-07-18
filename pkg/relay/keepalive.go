package relay

import (
	"log"
	"runtime/debug"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// safeGo 启动一个带 panic 恢复的 goroutine。panic 会被记录日志而不是让整个进程崩溃。
func safeGo(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[panic] %s: %v\n%s", name, r, debug.Stack())
			}
		}()
		fn()
	}()
}

// StartKeepAlive 启动一个后台 goroutine，以 interval 为间隔向服务端发送心跳帧。
// 如果发送失败（连接断开），goroutine 自动退出。
func StartKeepAlive(sess *session.Session, interval time.Duration) {
	safeGo("keepalive", func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := sess.Send(protocol.TypeKeepalive, nil); err != nil {
				return
			}
		}
	})
}

// StartServerKeepAlive 在服务端向客户端发送心跳帧，通过 stop channel 控制退出。
// 用于服务端检测客户端连接是否已断开（配合客户端的读超时）。
func StartServerKeepAlive(conn protocol.FrameReadWriter, interval time.Duration, stop <-chan struct{}) {
	safeGo("keepalive-server", func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.Send(protocol.TypeKeepalive, nil); err != nil {
					return
				}
			case <-stop:
				return
			}
		}
	})
}
