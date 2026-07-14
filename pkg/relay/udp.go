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

// udpReadLoop 在 forwarder 端运行，从目标 UDP 地址读取数据并发送到 Session。
func udpReadLoop(sess *session.Session, sid uint16, conn net.Conn, sm *StreamMap) {
	defer conn.Close()
	defer sess.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
	defer sm.Remove(sid)

	buf := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		if err := sess.Send(protocol.TypeDataUDP, payload); err != nil {
			return
		}
	}
}

// RunUDPListener 在 listener 端运行 UDP 监听循环。
// UDP 是无连接协议，通过源地址识别不同的"流"，每个唯一客户端地址分配一个 sid。
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

	// clients 记录 sid → 客户端地址的映射（UDP 无连接，按源地址分流）
	clients := make(map[uint16]*net.UDPAddr)
	var nextID uint16
	var mu sync.Mutex

	// 后台处理来自 forwarder 的帧
	go func() {
		for frame := range sess.FrameCh() {
			switch frame.Type {
			case protocol.TypeDataUDP:
				if len(frame.Payload) < 2 {
					continue
				}
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				data := frame.Payload[2:]
				mu.Lock()
				addr, ok := clients[sid]
				mu.Unlock()
				if ok {
					if _, err := conn.WriteToUDP(data, addr); err != nil {
						log.Printf("[UDP Listener] write to client error (sid=%d): %v", sid, err)
					}
				} else {
					log.Printf("[UDP Listener] received DATA_UDP for unknown sid=%d", sid)
				}
			case protocol.TypePeerLeave:
				if len(frame.Payload) >= 2 {
					sid := binary.BigEndian.Uint16(frame.Payload[:2])
					mu.Lock()
					delete(clients, sid)
					mu.Unlock()
					log.Printf("[UDP Listener] peer leave sid=%d", sid)
				}
			}
		}
	}()

	// Session 结束时关闭 UDP 监听
	go func() {
		<-sess.Done()
		log.Printf("[udp-listener] session done, closing conn")
		conn.Close()
	}()

	buf := make([]byte, 64*1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return fmt.Errorf("UDP listener closed: %w", err)
		}

		// 查找或创建 sid（按源地址识别 UDP 流）
		mu.Lock()
		var sid uint16
		found := false
		for id, addr := range clients {
			if addr.String() == remoteAddr.String() {
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
				log.Printf("[UDP Listener] send PEER_JOIN error: %v", err)
				continue
			}
			log.Printf("[UDP Listener] new client sid=%d from %s", sid, remoteAddr)
		}

		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		if err := sess.Send(protocol.TypeDataUDP, payload); err != nil {
			log.Printf("[UDP Listener] send DATA_UDP error: %v", err)
			continue
		}
	}
}

// RunUDPForwarder 在 forwarder 端运行 UDP 转发循环。
// 收到 PeerJoin → 连接目标地址并创建流。
// 收到 DataUDP → 写入对应连接。
// 收到 PeerLeave → 关闭对应连接。
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
				log.Printf("[UDP Forwarder] dial UDP error (sid=%d): %v", sid, err)
				sess.Send(protocol.TypePeerLeave, frame.Payload)
				continue
			}
			sm.AddWithID(conn, sid)
			log.Printf("[UDP Forwarder] new stream sid=%d connected to %s", sid, targetAddr)
			go udpReadLoop(sess, sid, conn, sm)
		case protocol.TypeDataUDP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			data := frame.Payload[2:]
			if c := sm.Get(sid); c != nil {
				if _, err := c.Write(data); err != nil {
					log.Printf("[UDP Forwarder] write to target error (sid=%d): %v", sid, err)
				}
			} else {
				log.Printf("[UDP Forwarder] DATA_UDP for unknown sid=%d", sid)
			}
		case protocol.TypePeerLeave:
			if len(frame.Payload) >= 2 {
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				if c := sm.Get(sid); c != nil {
					c.Close()
					sm.Remove(sid)
					log.Printf("[UDP Forwarder] peer leave sid=%d", sid)
				}
			}
		}
	}
	return fmt.Errorf("session closed")
}
