package gateway

import (
	"context"
	"sync"
	"time"
)

// DefaultCaller 是配额表中的通配调用方键；
// 当具体调用方未配置时回退到该配额（若未配置通配项则拒绝）。
const DefaultCaller = "*"

// CallerQuota 描述单个调用方的配额。
type CallerQuota struct {
	Caller          string
	RateLimitPerSec float64 // 令牌桶填充速率（QPS）
	Burst           int     // 令牌桶容量
	MaxInflight     int     // 最大并发请求数
}

// callerState 是每个调用方的限流状态，在多次请求间保持。
type callerState struct {
	mu       sync.Mutex
	tokens   float64
	last     time.Time
	inflight int
}

// QuotaManager 按调用方管理限流与并发槽。
// 每个调用方一把锁：取令牌与占并发槽在同一临界区完成，
// 因此并发竞争下也不会放行超出配额的请求。
type QuotaManager struct {
	mu     sync.RWMutex
	quotas map[string]CallerQuota
	states map[string]*callerState
}

// NewQuotaManager 创建配额管理器。
// quotas 中可用 DefaultCaller("*") 配置未知名调用方的默认配额。
func NewQuotaManager(quotas map[string]CallerQuota) *QuotaManager {
	cp := make(map[string]CallerQuota, len(quotas))
	for k, v := range quotas {
		if v.Caller == "" {
			v.Caller = k
		}
		cp[k] = v
	}
	return &QuotaManager{
		quotas: cp,
		states: make(map[string]*callerState),
	}
}

// stateFor 返回（或惰性创建）调用方状态；第二返回值表示是否存在配额。
func (qm *QuotaManager) stateFor(caller string) (*callerState, CallerQuota, bool) {
	qm.mu.RLock()
	q, ok := qm.quotas[caller]
	st := qm.states[caller]
	qm.mu.RUnlock()
	if !ok {
		q, ok = qm.quotas[DefaultCaller]
		if ok {
			qm.mu.RLock()
			st = qm.states[DefaultCaller]
			qm.mu.RUnlock()
			caller = DefaultCaller
		}
	}
	if !ok {
		return nil, CallerQuota{}, false
	}
	if st == nil {
		qm.mu.Lock()
		st = qm.states[caller]
		if st == nil {
			st = &callerState{tokens: float64(q.Burst), last: time.Now()}
			qm.states[caller] = st
		}
		qm.mu.Unlock()
	}
	return st, q, true
}

// Acquire 尝试为调用方占用一个请求配额：先检查并发槽，再取令牌。
// 任一条件不满足都返回带明确 Reason 的 *GatewayError，且不占用任何资源。
// 返回的 release 必须在请求结束时恰好调用一次。
func (qm *QuotaManager) Acquire(ctx context.Context, caller string) (func(), error) {
	st, q, ok := qm.stateFor(caller)
	if !ok {
		return nil, NewGatewayError(ReasonRateLimited,
			"caller %q has no quota configured (and no %q default quota)", caller, DefaultCaller)
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	// 令牌桶按流逝时间补充。
	now := time.Now()
	if !st.last.IsZero() {
		st.tokens += now.Sub(st.last).Seconds() * q.RateLimitPerSec
		if st.tokens > float64(q.Burst) {
			st.tokens = float64(q.Burst)
		}
	}
	st.last = now

	// 先判并发：槽满直接拒绝，且不消耗令牌。
	if st.inflight >= q.MaxInflight {
		return nil, NewGatewayError(ReasonTooManyInflight,
			"caller %q inflight %d reached limit %d", caller, st.inflight, q.MaxInflight)
	}

	// 再判令牌：桶空即限流。
	if st.tokens < 1 {
		return nil, NewGatewayError(ReasonRateLimited,
			"caller %q rate %.2f/s exhausted (tokens=%.3f)", caller, q.RateLimitPerSec, st.tokens)
	}

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
		if st.inflight > 0 {
			st.inflight--
		}
	}, nil
}

// Snapshot 返回某调用方当前的在途数与（补充后的）令牌数，供测试断言。
func (qm *QuotaManager) Snapshot(caller string) (inflight int, tokens float64, ok bool) {
	st, q, exists := qm.stateFor(caller)
	if !exists {
		return 0, 0, false
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	st.tokens += now.Sub(st.last).Seconds() * q.RateLimitPerSec
	if st.tokens > float64(q.Burst) {
		st.tokens = float64(q.Burst)
	}
	st.last = now
	return st.inflight, st.tokens, true
}
