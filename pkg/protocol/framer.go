package protocol

import "time"

// FrameReadWriter is the core transport abstraction. It represents a
// connection that can send and receive frames with deadlines.
//
// tunnel.Tunnel already implements this interface.
type FrameReadWriter interface {
	Send(typ byte, payload []byte) error
	Receive() (*Frame, error)
	Close() error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}
