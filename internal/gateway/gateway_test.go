package gateway

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/backend"
	"github.com/h3yow/model-inference-gateway-go/internal/ratelimit"
	"github.com/h3yow/model-inference-gateway-go/internal/retry"
	"github.com/h3yow/model-inference-gateway-go/internal/routing"
	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

type fixture struct {
	gw    *Gateway
	table *routing.Table
	logs  *bytes.Buffer
}

func newFixture(t *testing.T, stable, canary, fallback backend.Backend) *fixture {
	t.Helper()
	tab := routing.NewTable()
	_ = tab.Register("llm", "v1", stable.Name(), true)
	_ = tab.Register("llm", "v2", canary.Name(), true)
	_ = tab.SetCanary(routing.CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 0})

	limiter := ratelimit.New(ratelimit.CallerQuota{
		Name: "svc-a", RatePerSecond: 1000, BurstCapacity: 1000, MaxConcurrent: 2,
	})
	ex := retry.NewExecutor(retry.Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond})
	logs := &bytes.Buffer{}
	backs := []backend.Backend{stable, canary}
	if fallback != nil {
		backs = append(backs, fallback)
	}
	gw, err := New(Config{
		Limiter: limiter, Table: tab, Backends: backs,
		Executor: ex, Fallback: fallback, Logger: NewLogger(logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{gw: gw, table: tab, logs: logs}
}

func request(id, version string, idem bool) *types.Request {
	return &types.Request{ID: id, Caller: "svc-a", Model: "llm", Version: version, Idempotent: idem, Payload: "hello"}
}

func reason(t *testing.T, err error) types.Reason {
	t.Helper()
	if err == nil {
		return types.ReasonNone
	}
	ge, ok := err.(*types.GatewayError)
	if !ok {
		t.Fatalf("not a GatewayError: %v", err)
	}
	return ge.Reason
}

// TestGatewayOverloadRetryThenFallback 主后端持续过载：
// 幂等请求重试 3 次耗尽后走降级，响应必须显式标记 Degraded；非幂等请求只打 1 次。
func TestGatewayOverloadRetryThenFallback(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOverload, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fb := backend.NewSimulated("fallback", "v0", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, fb) // pct=0 -> 未指定版本走 v1(overload)

	// 未指定版本 + 幂等：重试耗尽 -> 降级成功
	resp, err := fx.gw.Infer(context.Background(), request("g-overload", "", true))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Degraded || resp.Source != "fallback" {
		t.Fatalf("degraded response expected, got %+v", resp)
	}
	if stable.CallCount() != 3 {
		t.Fatalf("stable calls=%d, want 3 retries", stable.CallCount())
	}
	t.Logf("请求g-overload 判定依据: 主后端overload重试3次后降级, Degraded=%v source=%s note=%q",
		resp.Degraded, resp.Source, resp.DegradeNote)

	// 非幂等：不重试，直接降级
	resp, err = fx.gw.Infer(context.Background(), request("g-noidem", "", false))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Degraded {
		t.Fatalf("non-idempotent failure should still degrade, got %+v", resp)
	}
	if stable.CallCount() != 4 {
		t.Fatalf("stable calls=%d, want 4 (no retry for non-idempotent)", stable.CallCount())
	}
	t.Logf("请求g-noidem 判定依据: 非幂等只尝试1次即降级, 主后端累计调用=%d", stable.CallCount())
}

// TestGatewayTimeoutNoFallback 无降级后端时，超时耗尽必须透出 retry_exhausted。
func TestGatewayTimeoutNoFallback(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeTimeout, time.Millisecond)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, nil)

	_, err := fx.gw.Infer(context.Background(), request("g-timeout", "", true))
	if got := reason(t, err); got != types.ReasonRetryExhausted {
		t.Fatalf("reason=%s, want retry_exhausted", got)
	}
	if stable.CallCount() != 3 {
		t.Fatalf("calls=%d want 3", stable.CallCount())
	}
	t.Logf("请求g-timeout 判定依据: timeout可重试, 3次耗尽且无降级后端 -> retry_exhausted")
}

// TestGatewayPartialFailureNonRetryable 部分失败不可重试：只尝试 1 次后降级。
func TestGatewayPartialFailureNonRetryable(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModePartial, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fb := backend.NewSimulated("fallback", "v0", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, fb)

	resp, err := fx.gw.Infer(context.Background(), request("g-partial", "", true))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Degraded {
		t.Fatalf("partial failure should degrade, got %+v", resp)
	}
	if stable.CallCount() != 1 {
		t.Fatalf("partial failure must not retry, calls=%d", stable.CallCount())
	}
	t.Logf("请求g-partial 判定依据: 部分失败不可重试, 1次后降级 Degraded=%v", resp.Degraded)
}

// TestGatewayFallbackAlsoFails 降级后端也失败时必须得到 fallback_failed。
func TestGatewayFallbackAlsoFails(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOverload, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fb := backend.NewSimulated("fallback", "v0", backend.ModePartial, 0)
	fx := newFixture(t, stable, canary, fb)

	_, err := fx.gw.Infer(context.Background(), request("g-fbfail", "", true))
	if got := reason(t, err); got != types.ReasonFallbackFailed {
		t.Fatalf("reason=%s, want fallback_failed", got)
	}
	t.Logf("请求g-fbfail 判定依据: 主后端重试耗尽且降级后端partial -> fallback_failed")
}

// TestGatewayCanarySwitch 灰度 0% -> 100% 切换前后，未指定版本的请求分别落到 v1/v2。
func TestGatewayCanarySwitch(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOK, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, nil)
	ctx := context.Background()

	// pct=0：全部稳定版本
	hits := map[string]int{}
	for i := 0; i < 20; i++ {
		resp, err := fx.gw.Infer(ctx, request(idFor(i), "", true))
		if err != nil {
			t.Fatal(err)
		}
		hits[resp.Version]++
	}
	if hits["v1"] != 20 || hits["v2"] != 0 {
		t.Fatalf("before switch hits=%v, want all v1", hits)
	}
	t.Logf("灰度0%%: 20个请求版本分布=%v", hits)

	// 滚动升级：切到 100%
	if err := fx.table.SetCanary(routing.CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 100}); err != nil {
		t.Fatal(err)
	}
	hits = map[string]int{}
	for i := 20; i < 40; i++ {
		resp, err := fx.gw.Infer(ctx, request(idFor(i), "", true))
		if err != nil {
			t.Fatal(err)
		}
		hits[resp.Version]++
	}
	if hits["v2"] != 20 || hits["v1"] != 0 {
		t.Fatalf("after switch hits=%v, want all v2", hits)
	}
	t.Logf("灰度100%%: 20个请求版本分布=%v（滚动升级完成）", hits)
}

// TestGatewayRoutingRejectionsEndToEnd 未注册版本与下线版本在入口处必须得到可区分原因。
func TestGatewayRoutingRejectionsEndToEnd(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOK, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, nil)
	ctx := context.Background()

	_, err := fx.gw.Infer(ctx, request("g-unreg", "v9", true))
	if got := reason(t, err); got != types.ReasonVersionUnregistered {
		t.Fatalf("reason=%s, want version_unregistered", got)
	}
	if err := fx.table.SetOffline("llm", "v2"); err != nil {
		t.Fatal(err)
	}
	_, err = fx.gw.Infer(ctx, request("g-offline", "v2", true))
	if got := reason(t, err); got != types.ReasonVersionOffline {
		t.Fatalf("reason=%s, want version_offline", got)
	}
	t.Logf("请求g-unreg/g-offline 分别得到 version_unregistered / version_offline，原因可区分")
}

// TestGatewayConcurrencyRejectEndToEnd 入口并发上限=2，20 个并发请求必须恰好 2 个进入后端。
func TestGatewayConcurrencyRejectEndToEnd(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOK, 20*time.Millisecond)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, nil)

	var entered int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	errReasons := sync.Map{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := fx.gw.Infer(context.Background(), request(idFor(100+i), "", true))
			if err != nil {
				errReasons.Store(string(reason(t, err)), true)
				return
			}
			atomic.AddInt64(&entered, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	if entered != 2 {
		t.Fatalf("entered backend=%d, want exactly 2 (max concurrency)", entered)
	}
	if _, ok := errReasons.Load(string(types.ReasonCallerConcurrency)); !ok {
		t.Fatal("some requests must be rejected with caller_concurrency")
	}
	t.Logf("判定依据: 并发上限2, 20并发请求进入后端=%d, 拒绝原因=caller_concurrency", entered)
}

// TestLogsCarryRequestIDAndBasis 每个被处理的请求都应在日志中留下请求标识与路由判定依据。
func TestLogsCarryRequestIDAndBasis(t *testing.T) {
	stable := backend.NewSimulated("stable", "v1", backend.ModeOK, 0)
	canary := backend.NewSimulated("canary", "v2", backend.ModeOK, 0)
	fb := backend.NewSimulated("fallback", "v0", backend.ModeOK, 0)
	fx := newFixture(t, stable, canary, fb)

	if _, err := fx.gw.Infer(context.Background(), request("g-log1", "", true)); err != nil {
		t.Fatal(err)
	}
	log := fx.logs.String()
	for _, want := range []string{"request=g-log1", "stage=ratelimit", "stage=routing", "stage=execute", "依据"} {
		if !bytes.Contains([]byte(log), []byte(want)) {
			t.Fatalf("log missing %q, full log:\n%s", want, log)
		}
	}
	t.Logf("链路日志:\n%s", log)
}

func idFor(i int) string {
	return "g-" + itoa(i)
}
