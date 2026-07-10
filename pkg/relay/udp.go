package relay

import (
	"encoding/binary"
	"net"
	"sync"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

func udpReadLoop(tun *tunnel.Tunnel, sid uint16, conn net.Conn, sm *StreamMap) {
	defer conn.Close()
	defer tun.Send(protocol.TypePeerLeave, uint16ToBytes(sid))
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
		if err := tun.Send(protocol.TypeDataUDP, payload); err != nil {
			return
		}
	}
}

func RunUDPListener(tun *tunnel.Tunnel, listenAddr string) error {
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

	go func() {
		for {
			frame, err := tun.Receive()
			if err != nil {
				return
			}
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
					conn.WriteToUDP(data, addr)
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
	}()

	buf := make([]byte, 64*1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return err
		}

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
			tun.Send(protocol.TypePeerJoin, uint16ToBytes(sid))
		}

		payload := make([]byte, 2+n)
		binary.BigEndian.PutUint16(payload[:2], sid)
		copy(payload[2:], buf[:n])
		tun.Send(protocol.TypeDataUDP, payload)
	}
}

func RunUDPForwarder(tun *tunnel.Tunnel, targetAddr string) error {
	sm := NewStreamMap()
	raddr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return err
	}

	for {
		frame, err := tun.Receive()
		if err != nil {
			return err
		}

		switch frame.Type {
		case protocol.TypePeerJoin:
			sid := binary.BigEndian.Uint16(frame.Payload)
			conn, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				tun.Send(protocol.TypePeerLeave, frame.Payload)
				continue
			}
			sm.AddWithId(conn, sid)
			go udpReadLoop(tun, sid, conn, sm)
		case protocol.TypeDataUDP:
			if len(frame.Payload) < 2 {
				continue
			}
			sid := binary.BigEndian.Uint16(frame.Payload[:2])
			data := frame.Payload[2:]
			if c := sm.Get(sid); c != nil {
				c.Write(data)
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
}
