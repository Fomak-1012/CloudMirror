package relay

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// 在 listener 端运行，处理来自 forwarder 的帧
// 负责将 DataTCP 写入对应的本地连接，PeerLeave 则关闭连接。
func handleForwarderFrames(sess *session.Session, sm *StreamMap) {
	for frame := range sess.FrameCh() {
		if len(frame.Payload) < 2 {
			continue
		}
		sid := binary.BigEndian.Uint16(frame.Payload[:2])

		switch frame.Type {
		case protocol.TypeDataTCP:
			if conn := sm.Get(sid); conn != nil {
				conn.Write(frame.Payload[2:])
			}
		case protocol.TypePeerLeave:
			if conn := sm.Get(sid); conn != nil {
				conn.Close()
				sm.Remove(sid)
			}
		}
	}
}

// connReadLoop 从目标连接持续读取数据，封装为 DataTCP 帧发送到 Session。
// 连接关闭或发送失败时自动清理 sid 并通知对端。
func connReadLoop(sess *session.Session, sid uint16, conn net.Conn, sm *StreamMap, bufSize int) {
	defer conn.Close()
	defer sess.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
	defer sm.Remove(sid)

	buf := make([]byte, bufSize)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		if err := sess.Send(protocol.TypeDataTCP, payload); err != nil {
			return
		}
	}
}

// 在 listener 端运行 TCP 监听循环
func RunTCPListener(sess *session.Session, ln net.Listener) error {
	defer ln.Close()

	sm := NewStreamMap()
	// 处理 Session 帧，写入本地连接
	safeGo("tcp-handleForwarderFrames", func() { handleForwarderFrames(sess, sm) })
	// 等待 Session 结束 -> 关闭监听器
	safeGo("tcp-listener-close", func() { <-sess.Done(); ln.Close() })

	for {
		// 接受新连接
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("listener closed: %w", err)
		}

		// 分配 sid
		sid := sm.Add(conn)
		if err := sess.Send(protocol.TypePeerJoin, uint16ToBytes(sid)); err != nil {
			log.Printf("[tcp] send PeerJoin error: %v", err)
			conn.Close()
			sm.Remove(sid)
			continue
		}

		// 为每个连接启动 connReadLoop
		safeGo("tcp-connReadLoop", func() {
			connReadLoop(sess, sid, conn, sm, 32*1024)
		})
	}
}

// 在 forwarder 端运行 TCP 转发循环
func RunTCPForwarder(sess *session.Session, targetAddr string) error {
	sm := NewStreamMap()

	for frame := range sess.FrameCh() {
		if len(frame.Payload) < 2 {
			continue
		}
		sid := binary.BigEndian.Uint16(frame.Payload[:2])

		// 处理帧类型
		switch frame.Type {
		// 建立到目标地址的新连接，启动 connReadLoop
		case protocol.TypePeerJoin:
			conn, err := net.Dial("tcp", targetAddr)
			if err != nil {
				sess.Send(protocol.TypePeerLeave, frame.Payload[:2])
				continue
			}
			sm.AddWithID(conn, sid)
			safeGo("tcp-connReadLoop", func() {
				connReadLoop(sess, sid, conn, sm, 32*1024)
			})

		// 写入对应的目标连接
		case protocol.TypeDataTCP:
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
