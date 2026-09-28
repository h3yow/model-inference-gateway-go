package gateway

import (
	"context"
	"testing"
	"time"
)

// assertReason 断言 err 的原因为 want，并打印请求标识与判定依据。
func assertReason(t *testing.T, reqID string, err error, want RejectReason) {
	t.Helper()
	got := ReasonOf(err)
	if got != want {
		t.Fatalf("[%s] reason mismatch: got=%q want=%q (err=%v)", reqID, got, want, err)
	}
	t.Logf("[%s] 判定依据: reason=%s, err=%v", reqID, got, err)
}

// noSleep 用于测试：跳过退避等待。
func noSleep(ctx context.Context, d time.Duration) error { return nil }

// newTestGateway 组装一个零退避的网关，方便快速测试重试。
func newTestGateway(t *testing.T, r *Router, qm *QuotaManager, maxAttempts int) (*Gateway, *[]Event) {
	t.Helper()
	events := &[]Event{}
	gw := New(
		r,
		qm,
		NewRetrier(RetryPolicy{MaxAttempts: maxAttempts, BaseDelay: 1, MaxDelay: 1, Factor: 2, Jitter: 0}),
		NewDegrader(DefaultFallback),
		withSleep(noSleep),
		WithEventHook(func(e Event) {
			*events = append(*events, e)
		}),
	)
	return gw, events
}

// logEvents 打印一次请求的完整判定链路。
func logEvents(t *testing.T, reqID string, events []Event) {
	t.Helper()
	for _, e := range events {
		if e.RequestID != reqID {
			continue
		}
		t.Logf("[%s] stage=%-8s reason=%-18s %s", e.RequestID, e.Stage, e.Reason, e.Message)
	}
}
