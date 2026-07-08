package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/Fomak-1012/CloudMirror/pkg/auth"
	"github.com/Fomak-1012/CloudMirror/pkg/protocol"
	"github.com/Fomak-1012/CloudMirror/pkg/relay"
	"github.com/Fomak-1012/CloudMirror/pkg/tunnel"
)

func resolvePort(spec string, index int) (int, error) {
	if !strings.Contains(spec, ",") {
		base, err := strconv.Atoi(spec)
		if err != nil {
			return 0, err
		}
		return base + index, nil
	}
	parts := strings.Split(spec, ",")
	ports := make([]int, len(parts))
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return 0, err
		}
		ports[i] = v
	}
	if index < len(ports) {
		return ports[index], nil
	}

	last := ports[len(ports)-1]
	return last + (index - len(ports) + 1), nil
}

func main() {
	serverIp := flag.String("s", "", "server address")
	serverPort := flag.Int("p", 3862, "server port")
	password := flag.String("e", "", "pre-shared password")
	forwardPort := flag.Int("f", 0, "forward target port (0 means not a forwarder)")
	listenPort := flag.String("l", "", "listener port")
	wantIndex := flag.Int("i", -1, "expected index (-1 means auto-assignment)")
	tcpOnly := flag.Bool("t", false, "only tcp")
	udpOnly := flag.Bool("u", false, "only udp")
	flag.Parse()
	log.Printf("DEBUG: forwardPort = %d", *forwardPort)

	if *serverIp == "" || *password == "" {
		log.Fatal("-s and -e must be provided")
	}

	if *tcpOnly && *udpOnly {
		log.Fatal("-t and -u cannot be used meanwhile")
	}

	role := ""
	if *forwardPort > 0 && *listenPort == "" {
		role = "forwarder"
	} else if *listenPort != "" && *forwardPort == 0 {
		role = "listener"
	} else {
		log.Fatal("-f or -l must be specified and cannot be specified meanwhile")
	}

	serverAddr := net.JoinHostPort(*serverIp, strconv.Itoa(*serverPort))
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		log.Fatalf("connection failed: %v", err)
	}
	tun := tunnel.NewTunnel(conn)
	defer tun.Close()

	if err := auth.ClientAuth(tun, *password); err != nil {
		log.Fatalf("auth failed: %v", err)
	}
	log.Println("auth pass")

	regPayload := role
	if *wantIndex >= 0 {
		regPayload = fmt.Sprintf("%s,%d", role, *wantIndex)
	}
	if err := tun.Send(protocol.TypeRegister, []byte(regPayload)); err != nil {
		log.Fatalf("sending register fails: %v", err)
	}

	frame, err := tun.Receive()
	if err != nil {
		log.Fatalf("receiving REG_OK fails: %v", err)
	}
	if frame.Type != protocol.TypeRegOK {
		log.Fatalf("sign is rejected: type = %d, msg = %s", frame.Type, string(frame.Payload))
	}
	assignedIndex, _ := strconv.Atoi(string(frame.Payload))
	log.Printf("sign pass, assigned index = %d", assignedIndex)

	switch role {
	case "listener":
		port, err := resolvePort(*listenPort, assignedIndex)
		if err != nil {
			log.Fatalf("parsing the listening port fails: %v", err)
		}
		addr := fmt.Sprintf(":%d", port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("listening fails: %v", err)
		}
		log.Printf("listeners running, listen %s", addr)
		relay.RunListener(tun, ln)
	case "forwarder":
		targetAddr := fmt.Sprintf("127.0.0.1:%d", *forwardPort)
		log.Printf("forwarder running, forward to %s", targetAddr)
		relay.RunForwarder(tun, targetAddr)
	}
}
