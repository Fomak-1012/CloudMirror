package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
)

func main() {
	port := flag.Int("p", 3862, "listening port")
	maxListeners := flag.Int("n", 0, "max number of listeners, 0 means no limit")
	password := flag.String("e", "", "pre-shared password")
	certlist := flag.String("s", "", "TLS certification lists")
	tunMode := flag.Bool("t", false, "enable TUN device mode")
	flag.Parse()

	if *password == "" {
		log.Fatal("please set password by -e")
	}

	srv := relay.NewServer(*password, *maxListeners)

	if *tunMode {
		if err := srv.StartTUNMode(); err != nil {
			log.Fatalf("TUN mode init failed: %v", err)
		}
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	var ln net.Listener
	var err error

	if *certlist != "" {
		pairs := strings.Split(*certlist, ";")
		var certs []tls.Certificate
		for _, pair := range pairs {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) != 2 {
				log.Fatalf("invalid certificate pair: %s", pair)
			}
			cert, err := tls.LoadX509KeyPair(parts[0], parts[1])
			if err != nil {
				log.Fatalf("certificate loading fails (%s): %v", pair, err)
			}
			certs = append(certs, cert)
		}
		tlsConfig := &tls.Config{Certificates: certs}
		ln, err = tls.Listen("tcp", addr, tlsConfig)
	} else {
		ln, err = net.Listen("tcp", addr)
	}

	if err != nil {
		log.Fatalf("listening fails: %v", err)
	}
	log.Printf("crfls running, listen %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go srv.HandleClient(conn)
	}
}
