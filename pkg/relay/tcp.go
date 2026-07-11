package relay

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

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

func RunTCPListener(sess *session.Session, ln net.Listener) error {
	defer ln.Close()

	sm := NewStreamMap()

	go handleServerFrames(sess, sm)

	go func() {
		<-sess.Done()
		log.Printf("[tcp-listener] session done, closing listener")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("listener closed: %v", err)
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
			sm.AddWithId(conn, sid)
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
