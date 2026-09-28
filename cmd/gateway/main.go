// Command gateway 启动一个本地运行的模型推理网关演示服务。
//
// 端到端本地验证：
//
//	go run ./cmd/gateway
//	curl -s -X POST localhost:8080/v1/infer -d '{"caller":"svc-a","model":"text-gen","idempotent":true,"payload":"hello"}'
//	curl -s -X POST localhost:8080/v1/infer -d '{"caller":"svc-a","model":"text-gen","version":"v2","idempotent":true}'
//
// 预置：
//   - 模型 text-gen：v1=stable(健康)，v2=canary(20% 流量，前两次注入过载后恢复，演示重试)
//   - 模型 flaky：v1 始终超时（幂等请求演示重试耗尽后降级）
//   - 模型 boom：v1 永久失败（演示不重试、不降级的硬失败）
//   - 调用方配额：svc-a 5/s、突发 5、并发 2；svc-b 2/s、突发 2、并发 1；* 默认 1/s
package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	gateway "github.com/h3yow/model-inference-gateway-go/internal/gateway"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	r := gateway.NewRouter()

	// text-gen：v1 健康稳定版；v2 灰度版，前两次调用注入过载后恢复。
	v1 := gateway.NewScriptedBackend("text-gen", "v1",
		gateway.ScriptedOutcome{Kind: gateway.FailureNone, Output: "v1 stable output"})
	v2 := gateway.NewFlakyBackend("text-gen", "v2", gateway.FailureOverload, 2)
	must(r.RegisterVersion(gateway.VersionInfo{Model: "text-gen", Version: "v1", State: gateway.VersionActive, Backend: v1}))
	must(r.RegisterVersion(gateway.VersionInfo{Model: "text-gen", Version: "v2", State: gateway.VersionCanary, Backend: v2}))
	must(r.SetRule(gateway.RouteRule{Model: "text-gen", Stable: "v1", StableWeight: 80, Canaries: map[string]int{"v2": 20}}))

	// flaky：v1 始终超时，用于演示可重试失败在重试耗尽后降级。
	tmo := gateway.NewScriptedBackend("flaky", "v1",
		gateway.ScriptedOutcome{Kind: gateway.FailureTimeout, Message: "simulated upstream timeout"})
	must(r.RegisterVersion(gateway.VersionInfo{Model: "flaky", Version: "v1", State: gateway.VersionActive, Backend: tmo}))
	must(r.SetRule(gateway.RouteRule{Model: "flaky", Stable: "v1", StableWeight: 100}))

	// boom：v1 永久失败，演示不重试/不降级的硬失败。
	boom := gateway.NewScriptedBackend("boom", "v1",
		gateway.ScriptedOutcome{Kind: gateway.FailurePermanent, Message: "bad request parameters"})
	must(r.RegisterVersion(gateway.VersionInfo{Model: "boom", Version: "v1", State: gateway.VersionActive, Backend: boom}))
	must(r.SetRule(gateway.RouteRule{Model: "boom", Stable: "v1", StableWeight: 100}))

	qm := gateway.NewQuotaManager(map[string]gateway.CallerQuota{
		"svc-a":               {Caller: "svc-a", RateLimitPerSec: 5, Burst: 5, MaxInflight: 2},
		"svc-b":               {Caller: "svc-b", RateLimitPerSec: 2, Burst: 2, MaxInflight: 1},
		gateway.DefaultCaller: {Caller: gateway.DefaultCaller, RateLimitPerSec: 1, Burst: 1, MaxInflight: 4},
	})

	rt := gateway.NewRetrier(gateway.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
		Factor:      2,
		Jitter:      0.1,
	})
	dg := gateway.NewDegrader(gateway.DefaultFallback)

	gw := gateway.New(r, qm, rt, dg,
		gateway.WithRequestTimeout(5*time.Second),
		gateway.WithEventHook(func(e gateway.Event) {
			logger.Printf("[%s] stage=%s reason=%s %s", e.RequestID, e.Stage, e.Reason, e.Message)
		}),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/infer", inferHandler(gw))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	addr := ":8080"
	logger.Printf("model inference gateway listening on %s", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("server error: %v", err)
	}
}

type inferRequest struct {
	RequestID  string `json:"request_id"`
	Caller     string `json:"caller"`
	Model      string `json:"model"`
	Version    string `json:"version"`
	Idempotent bool   `json:"idempotent"`
	Payload    string `json:"payload"`
}

type inferResponse struct {
	RequestID    string `json:"request_id"`
	Model        string `json:"model"`
	Version      string `json:"version"`
	Degraded     bool   `json:"degraded"`
	FallbackNote string `json:"fallback_note,omitempty"`
	Output       string `json:"output,omitempty"`
}

type errorResponse struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
	Error     string `json:"error"`
}

func inferHandler(gw *gateway.Gateway) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in inferRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Reason: "bad_request", Error: err.Error()})
			return
		}
		if in.Caller == "" {
			in.Caller = "anonymous"
		}
		resp, err := gw.Infer(r.Context(), &gateway.Request{
			RequestID:  in.RequestID,
			Caller:     in.Caller,
			Model:      in.Model,
			Version:    in.Version,
			Idempotent: in.Idempotent,
			Payload:    in.Payload,
		})
		if err != nil {
			reason := gateway.ReasonOf(err)
			writeJSON(w, statusForReason(reason), errorResponse{
				RequestID: in.RequestID,
				Reason:    string(reason),
				Error:     err.Error(),
			})
			return
		}
		writeJSON(w, http.StatusOK, inferResponse{
			RequestID:    resp.RequestID,
			Model:        resp.Model,
			Version:      resp.Version,
			Degraded:     resp.Degraded,
			FallbackNote: resp.FallbackNote,
			Output:       resp.Output,
		})
	}
}

func statusForReason(reason gateway.RejectReason) int {
	switch reason {
	case gateway.ReasonRateLimited, gateway.ReasonTooManyInflight, gateway.ReasonBackendOverload:
		return http.StatusTooManyRequests
	case gateway.ReasonUnknownModel, gateway.ReasonUnknownVersion:
		return http.StatusNotFound
	case gateway.ReasonVersionOffline, gateway.ReasonRouteConflict, gateway.ReasonNoCandidate:
		return http.StatusConflict
	case gateway.ReasonBackendTimeout:
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func must(err error) {
	if err != nil {
		log.Fatalf("startup config error: %v", err)
	}
}
