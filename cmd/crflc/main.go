package main

import (
	"flag"
	"log"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
)

func main() {
	serverIP := flag.String("s", "", "server address (IP or domain)")
	serverPort := flag.Int("p", 3862, "server port")
	password := flag.String("e", "", "pre-shared password")
	forwardPort := flag.Int("f", 0, "forward target port")
	listenPort := flag.String("l", "", "listener port")
	wantIndex := flag.Int("i", -1, "expected index (-1 = auto)")
	tcpOnly := flag.Bool("t", false, "TCP only")
	udpOnly := flag.Bool("u", false, "UDP only")
	tlsInsecure := flag.Bool("tls-insecure", false, "skip TLS verification")
	flag.Parse()

	if *serverIP == "" || *password == "" {
		log.Fatal("-s and -e must be provided")
	}
	if *tcpOnly && *udpOnly {
		log.Fatal("-t and -u cannot be used together")
	}

	err := relay.RunClient(*serverIP, *serverPort, *password,
		*forwardPort, *listenPort, *wantIndex, *udpOnly, *tlsInsecure)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
