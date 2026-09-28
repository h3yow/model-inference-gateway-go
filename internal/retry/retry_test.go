package retry

import (
	"context"
	"testing"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/backend"
	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

func newReq(idempotent bool) *types.Request {
	return &types.Request{ID: "req-retry", Caller: "c", Model: "m", Version: "v1", Idempotent: idempotent, Payload: "p"}
}

func noSleepExecutor(p Policy) *Executor {
	e := NewExecutor(p)
	e.SetSleeper(func(ctx context.Context, d time.Duration) error { return nil })
	return e
}

func TestRetrySucceedsAfterTransitiveOverload(t *testing.T) {
	b := backend.NewSimulated("b", "v1", backend.ModeOverload, 0)
	go func() {
		time.Sleep(10 * time.Millisecond) // 模拟后端稍后恢复
		b.SetFailureMode(backend.ModeOK)
	}()
	// 使用真实退避：首次4ms、第二次8ms，第3次尝试约在12ms，后端10ms已恢复。
	ex := NewExecutor(Policy{MaxAttempts: 5, InitialBackoff: 4 * time.Millisecond})

	resp, attempts, err := ex.Do(context.Background(), b, newReq(true))
	if err != nil {
		t.Fatal(err)
	}
	if attempts < 2 || attempts > 5 {
		t.Fatalf("attempts=%d, want 2..5", attempts)
	}
	if resp == nil || resp.Source != "b" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if b.CallCount() != int64(attempts) {
		t.Fatalf("backend calls=%d attempts=%d", b.CallCount(), attempts)
	}
	t.Logf("判定依据: 请求%s 超载后第%d次尝试成功（共调用后端%d次）", "req-retry", attempts, b.CallCount())
}

func TestNonIdempotentNeverRetried(t *testing.T) {
	b := backend.NewSimulated("b", "v1", backend.ModeOverload, 0)
	ex := noSleepExecutor(Policy{MaxAttempts: 5, InitialBackoff: time.Millisecond})

	_, attempts, err := ex.Do(context.Background(), b, newReq(false))
	if err == nil {
		t.Fatal("expected overload error")
	}
	ge := err.(*types.GatewayError)
	if ge.Reason != types.ReasonBackendOverload {
		t.Fatalf("reason=%s, want backend_overload", ge.Reason)
	}
	if attempts != 1 || b.CallCount() != 1 {
		t.Fatalf("non-idempotent must try once: attempts=%d calls=%d", attempts, b.CallCount())
	}
	t.Logf("判定依据: 非幂等请求失败原因=%s，尝试次数=%d（未重试）", ge.Reason, attempts)
}

func TestPartialFailureNotRetried(t *testing.T) {
	b := backend.NewSimulated("b", "v1", backend.ModePartial, 0)
	ex := noSleepExecutor(Policy{MaxAttempts: 5, InitialBackoff: time.Millisecond})

	_, attempts, err := ex.Do(context.Background(), b, newReq(true))
	ge := err.(*types.GatewayError)
	if ge.Reason != types.ReasonBackendFailed {
		t.Fatalf("reason=%s, want backend_failed", ge.Reason)
	}
	if attempts != 1 || b.CallCount() != 1 {
		t.Fatalf("non-retryable failure must try once: attempts=%d calls=%d", attempts, b.CallCount())
	}
	t.Logf("判定依据: 部分失败原因=%s（不可重试），尝试次数=%d", ge.Reason, attempts)
}

func TestRetryExhaustionPreservesLastReason(t *testing.T) {
	b := backend.NewSimulated("b", "v1", backend.ModeTimeout, 0)
	ex := noSleepExecutor(Policy{MaxAttempts: 3, InitialBackoff: time.Millisecond})

	_, attempts, err := ex.Do(context.Background(), b, newReq(true))
	ge := err.(*types.GatewayError)
	if ge.Reason != types.ReasonRetryExhausted {
		t.Fatalf("reason=%s, want retry_exhausted", ge.Reason)
	}
	if attempts != 3 || b.CallCount() != 3 {
		t.Fatalf("attempts=%d calls=%d, want 3/3", attempts, b.CallCount())
	}
	if msg := ge.Message; !contains(msg, "backend_timeout") {
		t.Fatalf("exhausted error must preserve last reason, got: %s", msg)
	}
	t.Logf("判定依据: 重试上限=3，耗尽错误保留末次原因 -> %s", ge.Message)
}

func TestBackoffSequenceIsExponentialAndCapped(t *testing.T) {
	ex := NewExecutor(Policy{MaxAttempts: 5, InitialBackoff: 10 * time.Millisecond, MaxBackoff: 25 * time.Millisecond})
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	var got []time.Duration
	for attempt := 1; attempt <= 4; attempt++ {
		got = append(got, ex.backoffFor(attempt))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoff[%d]=%s, want %s (full=%v)", i, got[i], want[i], got)
		}
	}
	t.Logf("判定依据: 指数退避序列=%v（上限25ms）", got)
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
