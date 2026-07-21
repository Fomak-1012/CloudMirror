// crfls 是 CloudMirror 中继服务端，负责接纳客户端连接、配对转发。
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
	"github.com/Fomak-1012/CloudMirror/pkg/relay/web"
)

func main() {
	port := flag.Int("p", 3862, "relay listening port")
	maxListeners := flag.Int("n", 1, "max number of listeners (default 1), 0 means no limit")
	password := flag.String("e", "", "pre-shared password")
	certlist := flag.String("s", "", "TLS cert list: cert:key[,cert2:key2...]")
	tunMode := flag.Bool("t", false, "enable TUN device mode")
	webPort := flag.Int("w", 0, "web console port (0 = disabled)")
	flag.Parse()

	if *password == "" {
		log.Fatal("please set password by -e")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	srv := relay.NewServer(*password, *maxListeners)

	if *tunMode {
		if err := srv.StartTUNMode(); err != nil {
			log.Fatalf("TUN mode init failed: %v", err)
		}
	}

	// 启动 Web 控制台
	if *webPort > 0 {
		addr := fmt.Sprintf(":%d", *webPort)
		notify, webSrv, err := web.Serve(addr, srv)
		if err != nil {
			log.Printf("[web] setup error: %v", err)
		} else {
			go func() {
				log.Printf("[web] listening on %s", addr)
				if err := webSrv.ListenAndServe(); err != http.ErrServerClosed {
					log.Printf("[web] error: %v", err)
				}
			}()
			// 将 SSE 推送回调注入 relay.Server，peers 变更时自动推送
			srv.SetPeerChangeCallback(notify)
			defer webSrv.Shutdown(context.Background())
		}
	}

	addr := fmt.Sprintf("0.0.0.0:%d", *port)
	var ln net.Listener
	var err error

	if *certlist != "" {
		pairs := strings.Split(*certlist, ",")
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

	go func() {
		<-ctx.Done()
		log.Printf("received signal, shutting down...")
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				log.Println("shutting down, waiting up to 30s for active connections...")
				srv.Shutdown(30 * time.Second)
				log.Println("server stopped")
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		go srv.HandleClient(conn)
	}
}
