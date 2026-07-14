package relay

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// handleServerFrames 在 listener 端运行，处理来自 forwarder 的帧。
// 根据帧类型分发：DataTCP 写入对应流连接，PeerLeave 关闭并清理流。
func handleServerFrames(sess *session.Session, sm *StreamMap) {
	for frame := range sess.FrameCh() {
		log.Printf("[tcp-handleServerFrames] got frame type=0x%x", frame.Type)
		switch frame.Type {
		case protocol.TypeDataTCP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			data := frame.Payload[2:]
			if conn := sm.Get(sid); conn != nil {
				conn.Write(data)
			}
		case protocol.TypePeerLeave:
			if len(frame.Payload) >= 2 {
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				if conn := sm.Get(sid); conn != nil {
					conn.Close()
					sm.Remove(sid)
				}
			}
		}
	}
}

// tcpReadLoop 在 forwarder 端运行，从目标 TCP 连接读取数据并发送到 Session。
// 当连接关闭时，发送 PeerLeave 通知 listener 清理对应流。
func tcpReadLoop(sess *session.Session, sid uint16, conn net.Conn, sm *StreamMap) {
	defer conn.Close()
	defer sess.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
	defer sm.Remove(sid)

	buf := make([]byte, 32*1024)
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

// RunTCPListener 在 listener 端运行 TCP 监听循环。
// 接受新连接 → 分配流 ID → 通知 forwarder（PeerJoin）→ 启动读写转发。
func RunTCPListener(sess *session.Session, ln net.Listener) error {
	defer ln.Close()

	sm := NewStreamMap()

	// 后台处理来自 forwarder 的帧
	go handleServerFrames(sess, sm)

	// 当 Session 结束时关闭监听器，释放端口
	go func() {
		<-sess.Done()
		log.Printf("[tcp-listener] session done, closing listener")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("listener closed: %w", err)
		}
		sid := sm.Add(conn)
		sess.Send(protocol.TypePeerJoin, uint16ToBytes(sid))

		go func(sid uint16, conn net.Conn) {
			defer conn.Close()
			defer sess.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
			defer sm.Remove(sid)

			buf := make([]byte, 32*1024)
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
		}(sid, conn)
	}
}

// RunTCPForwarder 在 forwarder 端运行 TCP 转发循环。
// 收到 PeerJoin → 连接目标地址并创建流 → 启动读循环。
// 收到 DataTCP → 写入对应流连接。
// 收到 PeerLeave → 关闭对应流连接。
func RunTCPForwarder(sess *session.Session, targetAddr string) error {
	sm := NewStreamMap()

	for frame := range sess.FrameCh() {
		switch frame.Type {
		case protocol.TypePeerJoin:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload)
			conn, err := net.Dial("tcp", targetAddr)
			if err != nil {
				sess.Send(protocol.TypePeerLeave, frame.Payload)
				continue
			}
			sm.AddWithID(conn, sid)
			go tcpReadLoop(sess, sid, conn, sm)
		case protocol.TypeDataTCP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			data := frame.Payload[2:]
			if conn := sm.Get(sid); conn != nil {
				conn.Write(data)
			}
		case protocol.TypePeerLeave:
			if len(frame.Payload) >= 2 {
				sid := binary.BigEndian.Uint16(frame.Payload[:2])
				if conn := sm.Get(sid); conn != nil {
					conn.Close()
					sm.Remove(sid)
				}
			}
		}
	}
	return fmt.Errorf("session closed")
}
