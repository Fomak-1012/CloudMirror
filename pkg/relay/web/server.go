// Package web 提供 CloudMirror 的 Web 控制台 HTTP 服务和 API 端点。
package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"

	"github.com/Fomak-1012/CloudMirror/pkg/relay"
)

//go:embed index.html
var staticFiles embed.FS

// PeersProvider 是 relay.Server 实现的接口，提供 peer 和统计信息。
type PeersProvider interface {
	GetPeers() relay.PeersResponse
	GetStats() relay.StatsResponse
}

// NotifyFunc 是 relay.Server 在 peers 变更时调用的回调函数。
type NotifyFunc func()

// Serve 创建 Web 控制台的 HTTP 服务，返回通知回调（供 relay.Server 在
// peers 变更时调用）和 *http.Server（供调用方 ListenAndServe / Shutdown）。
func Serve(addr string, provider PeersProvider) (notify NotifyFunc, srv *http.Server, err error) {
	hub := newSSEHub()

	mux := http.NewServeMux()

	// REST API
	mux.HandleFunc("/api/peers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(provider.GetPeers())
	})

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(provider.GetStats())
	})

	// SSE 实时推送
	mux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		ch := hub.subscribe()
		defer hub.unsubscribe(ch)

		// 连接后立即发送当前状态
		sendSSE(w, provider)

		for {
			select {
			case <-ch:
				sendSSE(w, provider)
			case <-r.Context().Done():
				return
			}
		}
	})

	// 静态文件
	content, err := fs.Sub(staticFiles, ".")
	if err != nil {
		return nil, nil, err
	}
	mux.Handle("/", http.FileServer(http.FS(content)))

	// 返回通知回调：relay.Server 在 peer 变更时调用
	notify = func() {
		hub.broadcast([]byte("changed"))
	}

	srv = &http.Server{Addr: addr, Handler: mux}
	log.Printf("[web] console ready on %s", addr)
	return notify, srv, nil
}

// sendSSE 将当前 peers 和 stats 作为 SSE 事件发送。
func sendSSE(w http.ResponseWriter, provider PeersProvider) {
	data := struct {
		Peers relay.PeersResponse `json:"peers"`
		Stats relay.StatsResponse `json:"stats"`
	}{
		Peers: provider.GetPeers(),
		Stats: provider.GetStats(),
	}
	jsonData, _ := json.Marshal(data)
	fmt.Fprintf(w, "data: %s\n\n", jsonData)
	w.(http.Flusher).Flush()
}
