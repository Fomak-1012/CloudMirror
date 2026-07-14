// crflc 是 CloudMirror 客户端，可作为 listener 或 forwarder 连接到服务端。
//
// 用法：
//
//	# 普通转发
//	crflc -s <server> -p <port> -e <password> -l <port>      # listener
//	crflc -s <server> -p <port> -e <password> -f <port>      # forwarder
//
//	# TUN 模式
//	crflc -s <server> -p <port> -e <password> -l <cidr>      # TUN listener
//
// 标志：
//
//	-s  服务端地址（IP 或域名）
//	-p  服务端端口（默认 3862）
//	-e  预共享密钥
//	-f  forwarder 目标端口
//	-l  listener 端口（或 CIDR 用于 TUN 模式）
//	-i  期望 index（-1 自动分配）
//	-t  仅 TCP
//	-u  仅 UDP
//	-tls-insecure  跳过 TLS 证书验证
package main

import (
	"flag"
	"log"
	"strings"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
)

func main() {
	serverIP := flag.String("s", "", "server address (IP or domain)")
	serverPort := flag.Int("p", 3862, "server port")
	password := flag.String("e", "", "pre-shared password")
	forwardPort := flag.Int("f", 0, "forward target port")
	listenPort := flag.String("l", "", "listener port or TUN CIDR (e.g. 192.168.1.0/24)")
	wantIndex := flag.Int("i", -1, "expected index (-1 = auto)")
	tcpOnly := flag.Bool("t", false, "TCP only")
	udpOnly := flag.Bool("u", false, "UDP only")
	tlsInsecure := flag.Bool("tls-insecure", false, "skip TLS verification")
	flag.Parse()

	// 参数校验
	if *serverIP == "" || *password == "" {
		log.Fatal("-s and -e must be provided")
	}
	if *tcpOnly && *udpOnly {
		log.Fatal("-t and -u cannot be used together")
	}

	// TUN 检测：-l 包含 "/" 则为 CIDR → TUN 模式
	isTUN := strings.Contains(*listenPort, "/")
	if isTUN && (*tcpOnly || *udpOnly) {
		log.Fatal("-t/-u cannot be used with TUN mode (CIDR in -l)")
	}

	err := relay.RunClient(*serverIP, *serverPort, *password,
		*forwardPort, *listenPort, *wantIndex, *udpOnly, *tlsInsecure)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
