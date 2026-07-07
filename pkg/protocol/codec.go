package protocol

import (
	"io"
)

func PutUint24(bt []byte, val uint32) {
	bt[0] = byte(val >> 16)
	bt[1] = byte(val >> 8)
	bt[2] = byte(val)
}

func Uint24(b []byte) uint32 {
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
}

func WriteFrame(wrt io.Writer, frameType byte, payload []byte) error {
	header := make([]byte, 4)
	header[0] = frameType
	PutUint24(header[1:], uint32(len(payload)))
	if _, err := wrt.Write(header); err != nil {
		return err
	}
	if _, err := wrt.Write(payload); err != nil {
		return err
	}
	return nil
}

func ReadFrame(rd io.Reader) (*Frame, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(rd, header); err != nil {
		return nil, err
	}
	frameType := header[0]
	length := Uint24(header[1:])
	payload := make([]byte, length)
	if _, err := io.ReadFull(rd, payload); err != nil {
		return nil, err
	}
	return &Frame{Type: frameType, Payload: payload}, nil
}
