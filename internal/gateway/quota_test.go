package gateway

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuotaSequentialRateLimit(t *testing.T) {
	qm := NewQuotaManager(map[string]CallerQuota{
		"c": {Caller: "c", RateLimitPerSec: 10, Burst: 3, MaxInflight: 100},
	})
	ctx := context.Background()

	// 突发容量 3：前 3 个通过，第 4 个立刻被限流。
	for i := 0; i < 3; i++ {
		rel, err := qm.Acquire(ctx, "c")
		if err != nil {
			t.Fatalf("acquire %d should pass: %v", i, err)
		}
		rel()
	}
	_, err := qm.Acquire(ctx, "c")
	assertReason(t, "sequential-4", err, ReasonRateLimited)

	// 等待令牌补充后应再次可用（补充 1 个令牌）。
	time.Sleep(150 * time.Millisecond)
	rel, err := qm.Acquire(ctx, "c")
	if err != nil {
		t.Fatalf("after refill should pass: %v", err)
	}
	rel()
}

func TestQuotaConcurrentInflightNoOverAdmit(t *testing.T) {
	const maxInflight = 5
	qm := NewQuotaManager(map[string]CallerQuota{
		"c": {Caller: "c", RateLimitPerSec: 100000, Burst: 100000, MaxInflight: maxInflight},
	})

	var peak int64
	var inflight int64
	var allowed int64
	var rejected int64
	var rejectReasons sync.Map // RejectReason -> *int64

	const goroutines = 200
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		reqID := "concurrent-" + strconv.Itoa(i)
		go func() {
			defer wg.Done()
			<-gate // 同时起跑，制造竞争
			release, err := qm.Acquire(context.Background(), "c")
			if err != nil {
				atomic.AddInt64(&rejected, 1)
				cnt, _ := rejectReasons.LoadOrStore(ReasonOf(err), new(int64))
				atomic.AddInt64(cnt.(*int64), 1)
				assertReason(t, reqID, err, ReasonTooManyInflight)
				return
			}
			cur := atomic.AddInt64(&inflight, 1)
			// 记录峰值。
			for {
				p := atomic.LoadInt64(&peak)
				if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
					break
				}
			}
			atomic.AddInt64(&allowed, 1)
			time.Sleep(20 * time.Millisecond) // 持有槽
			atomic.AddInt64(&inflight, -1)
			release()
		}()
	}
	close(gate)
	wg.Wait()

	t.Logf("并发限流判定: allowed=%d rejected=%d peakInflight=%d (上限=%d)",
		allowed, rejected, peak, maxInflight)
	rejectReasons.Range(func(k, v any) bool {
		t.Logf("  拒绝原因 %s: %d 次", k, *v.(*int64))
		return true
	})

	if allowed != maxInflight {
		t.Fatalf("放行数=%d, 必须恰好等于并发上限 %d", allowed, maxInflight)
	}
	if allowed+rejected != goroutines {
		t.Fatalf("放行+拒绝=%d, 应等于总请求 %d", allowed+rejected, goroutines)
	}
	if peak > maxInflight {
		t.Fatalf("峰值在途=%d 超过上限 %d（出现竞争超额放行）", peak, maxInflight)
	}
}

func TestQuotaUnknownCallerWithoutDefault(t *testing.T) {
	qm := NewQuotaManager(map[string]CallerQuota{
		"known": {Caller: "known", RateLimitPerSec: 1, Burst: 1, MaxInflight: 1},
	})
	_, err := qm.Acquire(context.Background(), "stranger")
	assertReason(t, "unknown-caller", err, ReasonRateLimited)
}

func TestQuotaStatePersistsAcrossRequests(t *testing.T) {
	qm := NewQuotaManager(map[string]CallerQuota{
		"c": {Caller: "c", RateLimitPerSec: 0, Burst: 2, MaxInflight: 10},
	})
	for i := 0; i < 2; i++ {
		rel, err := qm.Acquire(context.Background(), "c")
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		rel()
	}
	if _, err := qm.Acquire(context.Background(), "c"); err == nil {
		t.Fatal("速率为 0 且令牌耗尽后必须持续拒绝，状态未保持")
	} else {
		assertReason(t, "persist-3", err, ReasonRateLimited)
	}
}
