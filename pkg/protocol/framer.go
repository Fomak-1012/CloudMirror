package protocol

import (
	"net"
	"time"
)

// FrameReadWriter 是传输层的核心抽象接口，表示一个可以读写协议帧的连接。
type FrameReadWriter interface {
	Send(typ byte, payload []byte) error
	Receive() (*Frame, error)
	Close() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	RemoteAddr() net.Addr // 对端网络地址
}
