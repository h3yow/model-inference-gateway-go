// 本地可运行的模型推理服务网关演示程序。
//
// 启动后：
//
//	go run ./cmd/gateway
//
// 核心接口：
//
//	POST /v1/infer       推理入口（限流->路由->重试->降级）
//	POST /v1/admin/fail  动态调整模拟后端故障模式：{"backend":"v1","mode":"overload"}
//	POST /v1/admin/canary 动态调整灰度比例：{"canary_pct":20}
//	GET  /healthz        健康检查
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/backend"
	"github.com/h3yow/model-inference-gateway-go/internal/gateway"
	"github.com/h3yow/model-inference-gateway-go/internal/ratelimit"
	"github.com/h3yow/model-inference-gateway-go/internal/retry"
	"github.com/h3yow/model-inference-gateway-go/internal/routing"
	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

type inferRequest struct {
	ID         string `json:"id"`
	Caller     string `json:"caller"`
	Model      string `json:"model"`
	Version    string `json:"version"`
	Idempotent bool   `json:"idempotent"`
	Payload    string `json:"payload"`
}

type inferResponse struct {
	RequestID   string `json:"request_id"`
	Model       string `json:"model"`
	Version     string `json:"version"`
	Source      string `json:"source"`
	Degraded    bool   `json:"degraded"`
	DegradeNote string `json:"degrade_note,omitempty"`
	Attempts    int    `json:"attempts"`
	Output      string `json:"output"`
}

type errorResponse struct {
	RequestID string `json:"request_id"`
	Error     struct {
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"error"`
}

type server struct {
	gw       *gateway.Gateway
	table    *routing.Table
	backends map[string]*backend.Simulated
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	// ---- 模拟后端 -----------------------------------------------------------
	v1 := backend.NewSimulated("llm-v1", "v1", backend.ModeOK, 5*time.Millisecond)
	v2 := backend.NewSimulated("llm-v2", "v2", backend.ModeOK, 8*time.Millisecond)
	fb := backend.NewSimulated("llm-fallback", "v0", backend.ModeOK, 2*time.Millisecond)
	backends := map[string]*backend.Simulated{
		"v1": v1, "v2": v2, "fallback": fb,
	}

	// ---- 路由表：llm 有 v1/v2 两个在线版本，初始 100% 走 v1 -------------------
	tab := routing.NewTable()
	must(tab.Register("llm", "v1", "llm-v1", true))
	must(tab.Register("llm", "v2", "llm-v2", true))
	must(tab.SetCanary(routing.CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 0}))

	// ---- 按调用方的限流与并发配额 --------------------------------------------
	limiter := ratelimit.New(
		ratelimit.CallerQuota{Name: "team-a", RatePerSecond: 2, BurstCapacity: 3, MaxConcurrent: 2},
		ratelimit.CallerQuota{Name: "team-b", RatePerSecond: 10, BurstCapacity: 10, MaxConcurrent: 5},
	)

	// ---- 重试策略：最多 3 次（含首次），10ms 起步指数退避，上限 40ms ----------
	executor := retry.NewExecutor(retry.Policy{
		MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond,
	})

	gw, err := gateway.New(gateway.Config{
		Limiter:  limiter,
		Table:    tab,
		Backends: []backend.Backend{v1, v2, fb},
		Executor: executor,
		Fallback: fb,
		Logger:   gateway.NewStdLogger(),
	})
	if err != nil {
		log.Fatal(err)
	}

	srv := &server{gw: gw, table: tab, backends: backends}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/infer", srv.handleInfer)
	mux.HandleFunc("POST /v1/admin/fail", srv.handleFail)
	mux.HandleFunc("POST /v1/admin/canary", srv.handleCanary)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	httpSrv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Printf("model-inference-gateway listening on %s", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutting down, waiting for in-flight requests...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	gw.Wait()
	log.Printf("stopped")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (s *server) handleInfer(w http.ResponseWriter, r *http.Request) {
	var in inferRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid_request", err.Error())
		return
	}
	if in.ID == "" || in.Caller == "" || in.Model == "" {
		writeError(w, http.StatusBadRequest, in.ID, "invalid_request", "id, caller and model are required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	req := &types.Request{
		ID: in.ID, Caller: in.Caller, Model: in.Model,
		Version: in.Version, Idempotent: in.Idempotent, Payload: in.Payload,
	}
	resp, err := s.gw.Infer(ctx, req)
	if err != nil {
		ge, _ := err.(*types.GatewayError)
		reason := string(types.ReasonBackendFailed)
		msg := err.Error()
		reqID := in.ID
		if ge != nil {
			reason = string(ge.Reason)
			msg = ge.Message
			reqID = ge.RequestID
		}
		writeError(w, httpStatusFor(reason), reqID, reason, msg)
		return
	}

	writeJSON(w, http.StatusOK, inferResponse{
		RequestID:   resp.RequestID,
		Model:       resp.Model,
		Version:     resp.Version,
		Source:      resp.Source,
		Degraded:    resp.Degraded,
		DegradeNote: resp.DegradeNote,
		Attempts:    resp.Attempts,
		Output:      resp.Output,
	})
}

type failRequest struct {
	Backend string `json:"backend"` // v1 | v2 | fallback
	Mode    string `json:"mode"`    // ok | overload | timeout | partial
}

func (s *server) handleFail(w http.ResponseWriter, r *http.Request) {
	var in failRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid_request", err.Error())
		return
	}
	b, ok := s.backends[in.Backend]
	if !ok {
		writeError(w, http.StatusNotFound, "", "backend_not_found",
			fmt.Sprintf("backend %q not found, want one of v1/v2/fallback", in.Backend))
		return
	}
	mode, err := parseMode(in.Mode)
	if err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid_request", err.Error())
		return
	}
	b.SetFailureMode(mode)
	log.Printf("[admin] backend %s -> failure mode %s", in.Backend, in.Mode)
	writeJSON(w, http.StatusOK, map[string]string{"backend": in.Backend, "mode": in.Mode})
}

type canaryRequest struct {
	CanaryPct int `json:"canary_pct"`
}

func (s *server) handleCanary(w http.ResponseWriter, r *http.Request) {
	var in canaryRequest
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, "", "invalid_request", err.Error())
		return
	}
	if err := s.table.SetCanary(routing.CanaryRule{
		Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: in.CanaryPct,
	}); err != nil {
		ge, _ := err.(*types.GatewayError)
		writeError(w, http.StatusConflict, "", string(ge.Reason), ge.Message)
		return
	}
	log.Printf("[admin] canary percentage -> %d%%", in.CanaryPct)
	writeJSON(w, http.StatusOK, map[string]int{"canary_pct": in.CanaryPct})
}

func parseMode(m string) (backend.FailureMode, error) {
	switch m {
	case "ok":
		return backend.ModeOK, nil
	case "overload":
		return backend.ModeOverload, nil
	case "timeout":
		return backend.ModeTimeout, nil
	case "partial":
		return backend.ModePartial, nil
	default:
		return backend.ModeOK, fmt.Errorf("invalid mode %q, want ok|overload|timeout|partial", m)
	}
}

// httpStatusFor 把可区分的网关原因映射成 HTTP 状态码；原因本身保留在响应体中。
func httpStatusFor(reason string) int {
	switch types.Reason(reason) {
	case types.ReasonRateLimited, types.ReasonCallerConcurrency,
		types.ReasonBackendOverload, types.ReasonRetryExhausted:
		return http.StatusTooManyRequests // 429
	case types.ReasonVersionUnregistered, types.ReasonCallerNotConfigured,
		types.ReasonBackendNotFound:
		return http.StatusNotFound // 404
	case types.ReasonVersionOffline, types.ReasonCanaryConflict:
		return http.StatusConflict // 409
	case types.ReasonBackendTimeout:
		return http.StatusGatewayTimeout // 504
	case types.ReasonFallbackFailed, types.ReasonBackendFailed:
		return http.StatusBadGateway // 502
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, requestID, reason, msg string) {
	resp := errorResponse{RequestID: requestID}
	resp.Error.Reason = reason
	resp.Error.Message = msg
	writeJSON(w, status, resp)
}
