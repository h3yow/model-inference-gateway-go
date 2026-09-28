package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// CallerQuota 描述单个调用方的配额。
type CallerQuota struct {
	Name          string
	RatePerSecond float64 // 令牌桶填充速率
	BurstCapacity int     // 令牌桶容量（必须 >=1）
	MaxConcurrent int     // 并发上限（必须 >=1）
}

type callerState struct {
	quota      CallerQuota
	mu         sync.Mutex // 保护令牌桶字段与并发计数；桶与计数在同一临界区内更新
	tokens     float64
	lastRefill time.Time
	inflight   int
}

// Limiter 按调用方维护令牌桶限流与并发信号量。
// 所有判断与占用在同一把调用方锁内完成，保证并发竞争下绝不超额放行。
type Limiter struct {
	mu      sync.RWMutex
	callers map[string]*callerState
	now     func() time.Time
}

// New 创建限流器。
func New(quotas ...CallerQuota) *Limiter {
	l := &Limiter{
		callers: make(map[string]*callerState, len(quotas)),
		now:     time.Now,
	}
	for _, q := range quotas {
		if q.BurstCapacity < 1 {
			q.BurstCapacity = 1
		}
		if q.MaxConcurrent < 1 {
			q.MaxConcurrent = 1
		}
		l.callers[q.Name] = &callerState{
			quota:      q,
			tokens:     float64(q.BurstCapacity),
			lastRefill: l.now(),
		}
	}
	return l
}

// SetClock 注入时钟（测试用），并以新时钟为基准重置各调用方的填充时刻。
func (l *Limiter) SetClock(f func() time.Time) {
	l.mu.Lock()
	l.now = f
	now := f()
	for _, st := range l.callers {
		st.mu.Lock()
		st.lastRefill = now
		st.mu.Unlock()
	}
	l.mu.Unlock()
}

// Acquire 尝试为调用方 caller 获取一个请求名额。
// 判定顺序：调用方是否已配置 -> 令牌桶速率 -> 并发上限。
// 成功返回的 release 必须且只能调用一次，用于归还并发名额（令牌不退还）。
func (l *Limiter) Acquire(ctx context.Context, caller string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.RLock()
	st, ok := l.callers[caller]
	l.mu.RUnlock()
	if !ok {
		return nil, types.NewGatewayError("", types.ReasonCallerNotConfigured,
			"caller %q has no quota configuration", caller)
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	// 1) 按经过时间填充令牌桶。
	now := l.now()
	elapsed := now.Sub(st.lastRefill).Seconds()
	if elapsed > 0 {
		st.tokens += elapsed * st.quota.RatePerSecond
		if cap := float64(st.quota.BurstCapacity); st.tokens > cap {
			st.tokens = cap
		}
		st.lastRefill = now
	}

	// 2) 速率判定：没有令牌即拒绝（令牌不预占，失败不改变计数）。
	if st.tokens < 1 {
		return nil, types.NewGatewayError("", types.ReasonRateLimited,
			"caller %q exceeded rate limit %.2f/s (burst=%d)",
			caller, st.quota.RatePerSecond, st.quota.BurstCapacity)
	}

	// 3) 并发判定：达到上限即拒绝。
	if st.inflight >= st.quota.MaxConcurrent {
		return nil, types.NewGatewayError("", types.ReasonCallerConcurrency,
			"caller %q reached max concurrency %d", caller, st.quota.MaxConcurrent)
	}

	// 4) 同临界区内完成占用：扣令牌 + 占并发名额。
	st.tokens--
	st.inflight++

	var released bool
	return func() {
		st.mu.Lock()
		defer st.mu.Unlock()
		if released {
			return
		}
		released = true
		st.inflight--
	}, nil
}
