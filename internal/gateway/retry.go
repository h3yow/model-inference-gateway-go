package gateway

import (
	"errors"
	"math/rand"
	"time"
)

// RetryPolicy 描述重试上限与退避。
type RetryPolicy struct {
	MaxAttempts int           // 含首次在内的最大尝试次数（>=1）
	BaseDelay   time.Duration // 第一次重试前的退避
	MaxDelay    time.Duration // 退避上限
	Factor      float64       // 指数因子（如 2.0）
	Jitter      float64       // 抖动比例，取值 [0,1)；0 表示无抖动
}

// DefaultRetryPolicy 是网关使用的默认策略：最多 3 次尝试（首调 + 2 次重试），
// 100ms 起步、指数翻倍、上限 1s、±20% 抖动。
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
		Factor:      2.0,
		Jitter:      0.2,
	}
}

// Retrier 判定失败可否重试并计算退避。
type Retrier struct {
	policy RetryPolicy
	rng    *rand.Rand
}

// NewRetrier 创建重试器；非法字段会被收敛为安全默认值。
func NewRetrier(policy RetryPolicy) *Retrier {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = 100 * time.Millisecond
	}
	if policy.MaxDelay <= 0 {
		policy.MaxDelay = time.Second
	}
	if policy.Factor <= 1 {
		policy.Factor = 2
	}
	if policy.Jitter < 0 {
		policy.Jitter = 0
	}
	if policy.Jitter >= 1 {
		policy.Jitter = 0.99
	}
	return &Retrier{policy: policy, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// IsRetryableError 仅当错误属于可重试的后端失败分类时为真。
// 永久失败、以及非 *BackendError 的错误均不可重试。
func IsRetryableError(err error) bool {
	var be *BackendError
	if errors.As(err, &be) {
		return be.Kind.Retryable()
	}
	return false
}

// IsRetryable 判定第 attempt 次（从 1 起）失败后是否允许再试一次。
// 必须同时满足三个条件：
//  1. 失败属于可重试分类（过载/超时/瞬时）；
//  2. 请求声明幂等（非幂等请求绝不重试）；
//  3. 尚未达到 MaxAttempts。
func (rt *Retrier) IsRetryable(req *InferenceRequest, err error, attempt int) bool {
	if attempt >= rt.policy.MaxAttempts {
		return false
	}
	if req == nil || !req.Idempotent {
		return false
	}
	return IsRetryableError(err)
}

// Backoff 计算第 attempt 次失败后（attempt 从 1 起）的等待时长：
// min(BaseDelay * Factor^(attempt-1), MaxDelay)，再乘 [1-Jitter, 1+Jitter] 的抖动。
func (rt *Retrier) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(rt.policy.BaseDelay)
	for i := 1; i < attempt; i++ {
		d *= rt.policy.Factor
	}
	if d > float64(rt.policy.MaxDelay) {
		d = float64(rt.policy.MaxDelay)
	}
	if rt.policy.Jitter > 0 {
		// [-Jitter, +Jitter]
		mul := 1 + (rt.rng.Float64()*2-1)*rt.policy.Jitter
		d *= mul
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// MaxAttempts 暴露最大尝试次数，供编排层使用。
func (rt *Retrier) MaxAttempts() int { return rt.policy.MaxAttempts }
