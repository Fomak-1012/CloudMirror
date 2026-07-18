// crflc 是 CloudMirror 客户端，可作为 listener 或 forwarder 连接到服务端。
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"strings"
	"syscall"

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
	tlsMode := flag.Bool("tls", false, "use TLS with certificate verification")
	tlsInsecure := flag.Bool("tls-insecure", false, "skip TLS verification (testing only)")
	flag.Parse()

	if *serverIP == "" || *password == "" {
		log.Fatal("-s and -e must be provided")
	}
	if *tcpOnly && *udpOnly {
		log.Fatal("-t and -u cannot be used together")
	}
	if *tlsMode && *tlsInsecure {
		log.Fatal("-tls and -tls-insecure cannot be used together")
	}

	isTUN := strings.Contains(*listenPort, "/")
	if isTUN && (*tcpOnly || *udpOnly) {
		log.Fatal("-t/-u cannot be used with TUN mode (CIDR in -l)")
	}

	// 捕捉终止信号，Ctrl+C 时停止重连并退出
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err := relay.RunClient(ctx, *serverIP, *serverPort, *password,
		*forwardPort, *listenPort, *wantIndex, *udpOnly, *tlsMode, *tlsInsecure)
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
