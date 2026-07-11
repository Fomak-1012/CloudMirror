package relay

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

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
