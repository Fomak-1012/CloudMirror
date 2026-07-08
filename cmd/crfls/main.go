package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
)

func main() {
	port := flag.Int("p", 3862, "listening port")
	maxListeners := flag.Int("n", 0, "max number of listeners, 0 means no limit")
	password := flag.String("e", "", "pre-shared password")
	flag.Parse()

	if *password == "" {
		log.Fatal("please set password by -e")
	}

	srv := relay.NewServer(*password, *maxListeners)

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	ln, err := net.Listen("tcp", addr)
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
