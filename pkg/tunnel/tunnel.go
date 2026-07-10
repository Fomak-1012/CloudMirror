package tunnel

import (
	"net"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

type Tunnel struct {
	conn net.Conn
}

func NewTunnel(conn net.Conn) *Tunnel {
	return &Tunnel{conn: conn}
}

func (t *Tunnel) Send(frameType byte, payload []byte) error {
	return protocol.WriteFrame(t.conn, frameType, payload)
}

func (t *Tunnel) Receive() (*protocol.Frame, error) {
	return protocol.ReadFrame(t.conn)
}

func (t *Tunnel) Close() error {
	return t.conn.Close()
}

func (t *Tunnel) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}

func (t *Tunnel) SetReadDeadline(tm time.Time) error {
	return t.conn.SetDeadline(tm)
}

func (t *Tunnel) SetWriteDeadline(tm time.Time) error {
	return t.conn.SetWriteDeadline(tm)
}
