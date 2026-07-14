// crfls 是 CloudMirror 中继服务端，负责接纳客户端连接、配对转发。
//
// 用法：
//
//	crfls -p 3862 -e <password> [-n <max_listeners>] [-s <cert:key>] [-t]
//
// 标志：
//
//	-p  监听端口（默认 3862）
//	-n  最大 listener 数量（0 表示无限制）
//	-e  预共享密钥
//	-s  TLS 证书列表，格式 cert1:key1[;cert2:key2...]
//	-t  启用 TUN 模式
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

	// 创建服务实例
	srv := relay.NewServer(*password, *maxListeners)

	// 可选：启用 TUN 模式
	if *tunMode {
		if err := srv.StartTUNMode(); err != nil {
			log.Fatalf("TUN mode init failed: %v", err)
		}
	}

	// 根据是否指定证书选择 TLS 或普通 TCP 监听
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

	// 接受连接循环
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		go srv.HandleClient(conn)
	}
}
