package relay

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// handleServerFrames 在 listener 端运行，处理来自 forwarder 的帧。
func handleServerFrames(sess *session.Session, sm *StreamMap) {
	for frame := range sess.FrameCh() {
		switch frame.Type {
		case protocol.TypeDataTCP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			if conn := sm.Get(sid); conn != nil {
				conn.Write(frame.Payload[2:])
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

// connReadLoop 在 forwarder 端运行，从目标连接读取数据并发送到 Session。
// bufSize 指定读缓冲区大小（TCP 通常 32KB，UDP 通常 64KB）。
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

// RunTCPListener 在 listener 端运行 TCP 监听循环。
func RunTCPListener(sess *session.Session, ln net.Listener) error {
	defer ln.Close()

	sm := NewStreamMap()
	go handleServerFrames(sess, sm)

	// Session 结束时关闭监听器
	go func() {
		<-sess.Done()
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
			go connReadLoop(sess, sid, conn, sm, 32*1024)
		case protocol.TypeDataTCP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			if conn := sm.Get(sid); conn != nil {
				conn.Write(frame.Payload[2:])
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
