package relay

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// RunUDPListener 在 listener 端运行 UDP 监听循环。
// UDP 是无连接协议，按源地址识别不同的"流"，每个唯一客户端地址分配一个 sid。
func RunUDPListener(sess *session.Session, listenAddr string) error {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	clients := make(map[uint16]*net.UDPAddr)
	var nextID uint16
	var mu sync.Mutex

	// 后台处理来自 forwarder 的帧
	safeGo("udp-handleFrames", func() {
		for frame := range sess.FrameCh() {
			switch frame.Type {
			case protocol.TypeDataUDP:
				if len(frame.Payload) < 2 {
					continue
				}
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				mu.Lock()
				addr, ok := clients[sid]
				mu.Unlock()
				if ok {
					conn.WriteToUDP(frame.Payload[2:], addr)
				}
			case protocol.TypePeerLeave:
				if len(frame.Payload) >= 2 {
					sid := binary.BigEndian.Uint16(frame.Payload[:2])
					mu.Lock()
					delete(clients, sid)
					mu.Unlock()
				}
			}
		}
	})

	// Session 结束时关闭 UDP 监听
	safeGo("udp-listener-close", func() { <-sess.Done(); conn.Close() })

	buf := make([]byte, 64*1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return fmt.Errorf("UDP listener closed: %w", err)
		}

		mu.Lock()
		var sid uint16
		found := false
		for id, a := range clients {
			if a.String() == remoteAddr.String() {
				sid = id
				found = true
				break
			}
		}
		if !found {
			sid = nextID
			nextID++
			clients[sid] = remoteAddr
		}
		mu.Unlock()

		if !found {
			if err := sess.Send(protocol.TypePeerJoin, uint16ToBytes(sid)); err != nil {
				log.Printf("[udp] send PeerJoin error: %v", err)
				continue
			}
		}

		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		if err := sess.Send(protocol.TypeDataUDP, payload); err != nil {
			log.Printf("[udp] send error: %v", err)
			continue
		}
	}
}

// RunUDPForwarder 在 forwarder 端运行 UDP 转发循环。
func RunUDPForwarder(sess *session.Session, targetAddr string) error {
	sm := NewStreamMap()
	raddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return err
	}

	for frame := range sess.FrameCh() {
		switch frame.Type {
		case protocol.TypePeerJoin:
			sid := binary.BigEndian.Uint16(frame.Payload)
			conn, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				sess.Send(protocol.TypePeerLeave, frame.Payload)
				continue
			}
			sm.AddWithID(conn, sid)
			safeGo("udp-connReadLoop", func() { connReadLoop(sess, sid, conn, sm, 64*1024) })
		case protocol.TypeDataUDP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			if c := sm.Get(sid); c != nil {
				c.Write(frame.Payload[2:])
			}
		case protocol.TypePeerLeave:
			if len(frame.Payload) >= 2 {
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				if c := sm.Get(sid); c != nil {
					c.Close()
					sm.Remove(sid)
				}
			}
		}
	}
	return fmt.Errorf("session closed")
}
