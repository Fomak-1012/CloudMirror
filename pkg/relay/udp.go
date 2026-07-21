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

type udpClientMap struct {
	mu      sync.Mutex
	clients map[uint16]*net.UDPAddr
	nextID  uint16
}

func newUDPClientMap() *udpClientMap {
	return &udpClientMap{clients: make(map[uint16]*net.UDPAddr)}
}

// 根据远程地址查找已有的 sid，不存在则分配新的
// 返回 sid 和是否为新分配
func (m *udpClientMap) getOrAssign(addr *net.UDPAddr) (sid uint16, isNew bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	addrStr := addr.String()
	for id, a := range m.clients {
		if a.String() == addrStr {
			return id, false
		}
	}

	sid = m.nextID
	m.nextID++
	m.clients[sid] = addr
	return sid, true
}

func (m *udpClientMap) get(sid uint16) *net.UDPAddr {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.clients[sid]
}

func (m *udpClientMap) remove(sid uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, sid)
}

// 在 UDP listener 端处理来自 forwarder 的帧
func handleUDPForwarderFrames(sess *session.Session, conn *net.UDPConn, clients *udpClientMap) {
	for frame := range sess.FrameCh() {
		if len(frame.Payload) < 2 {
			continue
		}
		sid := binary.BigEndian.Uint16(frame.Payload[:2])

		switch frame.Type {
		case protocol.TypeDataUDP:
			if addr := clients.get(sid); addr != nil {
				conn.WriteToUDP(frame.Payload[2:], addr)
			}
		case protocol.TypePeerLeave:
			clients.remove(sid)
		}
	}
}

// 在 listener 端运行 UDP 监听循环
// 注：UDP 是无连接协议，每个唯一的客户端地址分配一个 sid
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

	clients := newUDPClientMap()

	// 处理 Session 帧，写入本地 UDP
	safeGo("udp-handleForwarderFrames", func() { handleUDPForwarderFrames(sess, conn, clients) })

	// 等待 Session 结束 -> 关闭监听器
	safeGo("udp-listener-close", func() { <-sess.Done(); conn.Close() })

	buf := make([]byte, 64*1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return fmt.Errorf("UDP listener closed: %w", err)
		}

		sid, isNew := clients.getOrAssign(remoteAddr)

		if isNew {
			if err := sess.Send(protocol.TypePeerJoin, uint16ToBytes(sid)); err != nil {
				log.Printf("[udp] send PeerJoin error: %v", err)
				continue
			}
		}

		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		if err := sess.Send(protocol.TypeDataUDP, payload); err != nil {
			log.Printf("[udp] send DataUDP error: %v", err)
			continue
		}
	}
}

// 在 forwarder 端运行 UDP 转发循环
func RunUDPForwarder(sess *session.Session, targetAddr string) error {
	sm := NewStreamMap()
	raddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return err
	}

	for frame := range sess.FrameCh() {
		if len(frame.Payload) < 2 {
			continue
		}
		sid := binary.BigEndian.Uint16(frame.Payload[:2])

		switch frame.Type {
		// 建立到目标地址的 UDP "连接"，启动 connReadLoop
		case protocol.TypePeerJoin:
			conn, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				sess.Send(protocol.TypePeerLeave, frame.Payload[:2])
				continue
			}
			sm.AddWithID(conn, sid)
			safeGo("udp-connReadLoop", func() {
				connReadLoop(sess, sid, conn, sm, 64*1024)
			})

		// 写入对应的目标地址
		case protocol.TypeDataUDP:
			if conn := sm.Get(sid); conn != nil {
				conn.Write(frame.Payload[2:])
			}

		// 关闭对应的目标连接
		case protocol.TypePeerLeave:
			if conn := sm.Get(sid); conn != nil {
				conn.Close()
				sm.Remove(sid)
			}
		}
	}
	return fmt.Errorf("session closed")
}
