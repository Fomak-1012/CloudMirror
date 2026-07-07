package auth

import (
	"errors"

	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

func ClientAuth(t *tunnel.Tunnel, password string) error {
	if err := t.Send(protocol.TypeAuth, []byte(password)); err != nil {
		return err
	}
	frame, err := t.Receive()
	if err != nil {
		return err
	}
	if frame.Type != protocol.TypeAuthOK {
		return errors.New("auth failed: " + string(frame.Payload))
	}
	return nil
}

func ServerAuth(t *tunnel.Tunnel, expectedPassword string) error {
	frame, err := t.Receive()
	if err != nil {
		return err
	}
	if frame.Type != protocol.TypeAuth {
		return errors.New("expected AUTH frame")
	}
	if string(frame.Payload) != expectedPassword {
		t.Send(protocol.TypeError, []byte("password mismatch"))
		return errors.New("auth failed: wrong password")
	}
	return t.Send(protocol.TypeAuthOK, nil)
}
