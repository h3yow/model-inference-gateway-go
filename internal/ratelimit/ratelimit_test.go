package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// TestConcurrencyLimitNoOverAdmit 并发竞争下不得超额放行：
// 50 个 goroutine 同时抢 3 个并发名额，成功者必须恰好为 3。
func TestConcurrencyLimitNoOverAdmit(t *testing.T) {
	l := New(CallerQuota{Name: "svc-a", RatePerSecond: 1000, BurstCapacity: 1000, MaxConcurrent: 3})

	var granted int64
	var rejected int64
	var peak int64
	var current int64

	const n = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 同时发起，制造竞争
			release, err := l.Acquire(context.Background(), "svc-a")
			if err != nil {
				ge := err.(*types.GatewayError)
				if ge.Reason != types.ReasonCallerConcurrency {
					t.Errorf("unexpected reject reason: %s", ge.Reason)
				}
				atomic.AddInt64(&rejected, 1)
				return
			}
			atomic.AddInt64(&granted, 1)
			c := atomic.AddInt64(&current, 1)
			for {
				p := atomic.LoadInt64(&peak)
				if c <= p || atomic.CompareAndSwapInt64(&peak, p, c) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt64(&current, -1)
			release()
		}()
	}
	close(start)
	wg.Wait()

	if granted != 3 {
		t.Fatalf("granted=%d, want 3 (no over-admit under contention)", granted)
	}
	if rejected != n-3 {
		t.Fatalf("rejected=%d, want %d", rejected, n-3)
	}
	if peak > 3 {
		t.Fatalf("peak inflight=%d, want <= 3", peak)
	}
	t.Logf("判定依据: 并发上限=3, 总请求=%d, 放行=%d, 限流拒绝=%d, 峰值并发=%d",
		n, granted, rejected, peak)
}

// TestRateLimitConsistentAcrossRequests 速率限流在多次请求间保持一致：
// 桶容量 5、速率 10/s，前 5 个放行，随后立即拒绝，推进时钟 100ms（补 1 个令牌）后恰好再放行 1 个。
func TestRateLimitConsistentAcrossRequests(t *testing.T) {
	clock := time.Now()
	l := New(CallerQuota{Name: "svc-b", RatePerSecond: 10, BurstCapacity: 5, MaxConcurrent: 100})
	l.SetClock(func() time.Time { return clock })

	allow := func(caller string) bool {
		release, err := l.Acquire(context.Background(), caller)
		if err != nil {
			return false
		}
		release()
		return true
	}

	for i := 0; i < 5; i++ {
		if !allow("svc-b") {
			t.Fatalf("request %d should be allowed within burst", i+1)
		}
	}
	if allow("svc-b") {
		t.Fatal("6th immediate request must be rate limited")
	}

	clock = clock.Add(100 * time.Millisecond) // +1 token
	if !allow("svc-b") {
		t.Fatal("after 100ms at 10/s, exactly 1 token should be available")
	}
	if allow("svc-b") {
		t.Fatal("second request after single-token refill must be limited")
	}
	t.Logf("判定依据: burst=5, refill=10/s, 时钟推进100ms后恰好补1个令牌，限流状态跨请求一致")
}

// TestRejectReasonsAreDistinct 限流与并发拒绝原因必须可区分。
func TestRejectReasonsAreDistinct(t *testing.T) {
	// 场景一：令牌充足但并发已满 -> caller_concurrency
	cl := New(CallerQuota{Name: "svc-conc", RatePerSecond: 1000, BurstCapacity: 1000, MaxConcurrent: 1})
	first, err := cl.Acquire(context.Background(), "svc-conc")
	if err != nil {
		t.Fatal(err)
	}
	_, err = cl.Acquire(context.Background(), "svc-conc")
	ge := err.(*types.GatewayError)
	t.Logf("并发已满时拒绝原因=%s msg=%q", ge.Reason, ge.Message)
	if ge.Reason != types.ReasonCallerConcurrency {
		t.Fatalf("want caller_concurrency, got %s", ge.Reason)
	}
	first()

	// 场景二：并发空闲但令牌耗尽 -> rate_limited
	rl := New(CallerQuota{Name: "svc-rate", RatePerSecond: 0, BurstCapacity: 1, MaxConcurrent: 10})
	release, err := rl.Acquire(context.Background(), "svc-rate")
	if err != nil {
		t.Fatal(err)
	}
	release() // 归还并发名额，但令牌不退还
	_, err = rl.Acquire(context.Background(), "svc-rate")
	ge = err.(*types.GatewayError)
	t.Logf("令牌耗尽时拒绝原因=%s msg=%q", ge.Reason, ge.Message)
	if ge.Reason != types.ReasonRateLimited {
		t.Fatalf("want rate_limited, got %s", ge.Reason)
	}

	// 场景三：调用方未配置
	_, err = cl.Acquire(context.Background(), "unknown")
	ge = err.(*types.GatewayError)
	if ge.Reason != types.ReasonCallerNotConfigured {
		t.Fatalf("want caller_not_configured, got %s", ge.Reason)
	}
}
