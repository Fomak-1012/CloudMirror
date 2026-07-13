package relay

import (
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/session"
)

// Mode abstracts a relay forwarding mode (TCP/UDP listener, TCP/UDP forwarder, TUN).
type Mode interface {
	// Run starts the mode's forwarding loop on the given session.
	// It blocks until the session closes or a fatal error occurs.
	Run(sess *session.Session, index int) error
}

// --- TCP Listener ---

type tcpListenMode struct{ addr string }

func (m tcpListenMode) Run(sess *session.Session, index int) error {
	ln, err := net.Listen("tcp", m.addr)
	if err != nil {
		return fmt.Errorf("tcp listen: %w", err)
	}
	defer ln.Close()
	log.Printf("TCP listener running, listen %s", m.addr)
	return RunTCPListener(sess, ln)
}

// --- UDP Listener ---

type udpListenMode struct{ addr string }

func (m udpListenMode) Run(sess *session.Session, index int) error {
	log.Printf("UDP listener running, listen %s", m.addr)
	return RunUDPListener(sess, m.addr)
}

// --- TCP Forwarder ---

type tcpForwardMode struct{ target string }

func (m tcpForwardMode) Run(sess *session.Session, index int) error {
	log.Printf("TCP forwarder running, forward to %s", m.target)
	return RunTCPForwarder(sess, m.target)
}

// --- UDP Forwarder ---

type udpForwardMode struct{ target string }

func (m udpForwardMode) Run(sess *session.Session, index int) error {
	log.Printf("UDP forwarder running, forward to %s", m.target)
	return RunUDPForwarder(sess, m.target)
}

// --- TUN Listener ---

type tunListenMode struct {
	cidr       string
	assignedIP string
}

func (m tunListenMode) Run(sess *session.Session, index int) error {
	log.Printf("TUN listener running, IP=%s, CIDR=%s", m.assignedIP, m.cidr)
	return runTUNListener(sess, m.cidr, index, m.assignedIP)
}

