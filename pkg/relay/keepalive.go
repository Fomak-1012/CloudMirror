package relay

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// StartKeepAlive 启动一个后台 goroutine，以 interval 为间隔向服务端发送心跳帧。
// 如果发送失败（连接断开），goroutine 自动退出。
func StartKeepAlive(sess *session.Session, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := sess.Send(protocol.TypeKeepalive, nil); err != nil {
				log.Printf("[keepalive] send error: %v", err)
				return
			}
		}
	}()
}
