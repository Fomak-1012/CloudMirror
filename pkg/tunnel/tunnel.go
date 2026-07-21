package tunnel

import (
	"net"
	"sync"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

type Tunnel struct {
	conn    net.Conn
	writeMu sync.Mutex
}

func NewTunnel(conn net.Conn) *Tunnel {
	return &Tunnel{conn: conn}
}

// 将一帧编码后写入底层连接
func (t *Tunnel) Send(frameType byte, payload []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	return protocol.WriteFrame(t.conn, frameType, payload)
}

// 从连接读取并解码一帧
func (t *Tunnel) Receive() (*protocol.Frame, error) {
	return protocol.ReadFrame(t.conn)
}

// 关闭连接
func (t *Tunnel) Close() error {
	return t.conn.Close()
}

// 返回对端网络地址
func (t *Tunnel) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}

// 设置读取超时截止时间
func (t *Tunnel) SetReadDeadline(tm time.Time) error {
	return t.conn.SetReadDeadline(tm)
}

// 设置写入超时截止时间
func (t *Tunnel) SetWriteDeadline(tm time.Time) error {
	return t.conn.SetWriteDeadline(tm)
}
