package protocol

import "time"

// FrameReadWriter 是传输层的核心抽象接口，表示一个可以读写协议帧的连接。
// tunnel.Tunnel 是该接口的默认实现（基于 net.Conn）。
type FrameReadWriter interface {
	// Send 发送一帧数据。
	Send(typ byte, payload []byte) error

	// Receive 阻塞接收一帧数据。
	Receive() (*Frame, error)

	// Close 关闭连接。
	Close() error

	// SetReadDeadline 设置读取超时截止时间。
	SetReadDeadline(t time.Time) error

	// SetWriteDeadline 设置写入超时截止时间。
	SetWriteDeadline(t time.Time) error
}
