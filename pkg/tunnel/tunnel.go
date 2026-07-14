// Package tunnel 将 net.Conn 包装为协议帧读写器，实现了 protocol.FrameReadWriter 接口。
package tunnel

import (
	"net"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
)

// Tunnel 在 net.Conn 之上提供协议帧的收发能力，实现了 protocol.FrameReadWriter 接口。
// 它是 Session 的底层传输载体。
type Tunnel struct {
	conn net.Conn // 底层 TCP/TLS 连接
}

// NewTunnel 基于已有连接创建一个 Tunnel。
func NewTunnel(conn net.Conn) *Tunnel {
	return &Tunnel{conn: conn}
}

// Send 将一帧编码后写入底层连接。
func (t *Tunnel) Send(frameType byte, payload []byte) error {
	return protocol.WriteFrame(t.conn, frameType, payload)
}

// Receive 从底层连接读取并解码一帧。
func (t *Tunnel) Receive() (*protocol.Frame, error) {
	return protocol.ReadFrame(t.conn)
}

// Close 关闭底层连接。
func (t *Tunnel) Close() error {
	return t.conn.Close()
}

// RemoteAddr 返回对端网络地址。
func (t *Tunnel) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}

// SetReadDeadline 设置读取超时截止时间。
func (t *Tunnel) SetReadDeadline(tm time.Time) error {
	return t.conn.SetDeadline(tm)
}

// SetWriteDeadline 设置写入超时截止时间。
func (t *Tunnel) SetWriteDeadline(tm time.Time) error {
	return t.conn.SetWriteDeadline(tm)
}
