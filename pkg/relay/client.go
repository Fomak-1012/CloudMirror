package relay

import (
	"encoding/binary"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

func handleServerFrames(tun *tunnel.Tunnel, sm *StreamMap) {
	for {
		frame, err := tun.Receive()
		if err != nil {
			return
		}
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

func uint16ToBytes(val uint16) []byte {
	b := make([]byte, 2)
	b[0] = byte(val >> 8)
	b[1] = byte(val)
	return b
}

func RunListener(tun *tunnel.Tunnel, listnenAddr string) error {
	ln, err := net.Listen("tcp", listnenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	sm := NewStreamMap()

	go handleServerFrames(tun, sm)

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		sid := sm.Add(conn)
		tun.Send(protocol.TypePeerJoin, uint16ToBytes(sid))

		go func(sid uint16, conn net.Conn) {
			defer conn.Close()
			defer tun.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
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
				if err := tun.Send(protocol.TypeDataTCP, payload); err != nil {
					return
				}
			}
		}(sid, conn)
	}
}

func forwardReadLoop(tun *tunnel.Tunnel, sid uint16, conn net.Conn, sm *StreamMap) {
	defer conn.Close()
	defer tun.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
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
		if err := tun.Send(protocol.TypeDataTCP, payload); err != nil {
			return
		}
	}
}

func RunForwarder(tun *tunnel.Tunnel, targetAddr string) error {
	sm := NewStreamMap()

	for {
		frame, err := tun.Receive()
		if err != nil {
			return err
		}
		switch frame.Type {
		case protocol.TypePeerJoin:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload)
			conn, err := net.Dial("tcp", targetAddr)
			if err != nil {
				tun.Send(protocol.TypePeerLeave, frame.Payload)
				continue
			}
			sm.AddWithId(conn, sid)
			go forwardReadLoop(tun, sid, conn, sm)
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
