package relay

import (
	"log"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

func StartKeepAlive(tun *tunnel.Tunnel, interval, timeout time.Duration, onTimeout func()) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		go func() {
			for range ticker.C {
				if err := tun.Send(protocol.TypeKeepalive, nil); err != nil {
					log.Printf("[KeepAlive] send keepalive error: %v", err)
					return
				}
			}
		}()

		for {
			tun.SetReadDeadline(time.Now().Add(timeout))
			_, err := tun.Receive()
			if err != nil {
				log.Printf("[KeepAlive] receive error (timeout or conn closed): %v err")
				onTimeout()
				return
			}
		}
	}()
}
