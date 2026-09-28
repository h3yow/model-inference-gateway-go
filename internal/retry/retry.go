package retry

import (
	"context"
	"math"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/backend"
	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// Policy 描述重试策略：最大尝试次数与指数退避。
type Policy struct {
	MaxAttempts    int           // 含首次在内的总尝试次数上限（<1 视为 1，即不重试）
	InitialBackoff time.Duration // 第 1 次重试前的退避
	MaxBackoff     time.Duration // 退避上限（<=0 时不设上限）
}

// Executor 在同一后端上执行有限次重试。
type Executor struct {
	policy  Policy
	sleeper func(ctx context.Context, d time.Duration) error
}

// NewExecutor 创建重试执行器，默认退避使用真实时间。
func NewExecutor(policy Policy) *Executor {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	return &Executor{
		policy:  policy,
		sleeper: sleepCtx,
	}
}

// SetSleeper 注入退避等待函数（测试中可跳过实际等待）。
func (e *Executor) SetSleeper(f func(ctx context.Context, d time.Duration) error) {
	e.sleeper = f
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Do 按策略调用 b.Infer。
//
// 重试只有在【同时】满足以下条件时才发生：
//  1. req.Idempotent == true（非幂等请求失败即返回，绝不重试）；
//  2. 错误是 *GatewayError 且 Retryable()==true（当前为 overload/timeout）；
//  3. 尚未达到 MaxAttempts。
//
// 退避为指数退避：min(InitialBackoff * 2^(retry-1), MaxBackoff)。
// 重试耗尽时返回 retry_exhausted，消息中保留最后一次的具体原因。
// 返回值 attempts 为实际尝试次数。
func (e *Executor) Do(ctx context.Context, b backend.Backend, req *types.Request) (*types.Response, int, error) {
	var lastErr error

	for attempt := 1; attempt <= e.policy.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, attempt - 1, types.NewGatewayError(req.ID, types.ReasonBackendTimeout,
				"retry loop canceled before attempt %d: %v", attempt, err)
		}

		resp, err := b.Infer(ctx, req)
		if err == nil {
			return resp, attempt, nil
		}
		lastErr = err

		// 非网关错误：不理解、不重试。
		ge, ok := err.(*types.GatewayError)
		if !ok {
			return nil, attempt, err
		}
		// 非幂等：立即失败，不做任何重试。
		if !req.Idempotent {
			return nil, attempt, ge
		}
		// 幂等但失败类型不可重试：立即失败。
		if !ge.Retryable() {
			return nil, attempt, ge
		}
		// 已是最后一次：不再等待，直接判定耗尽。
		if attempt == e.policy.MaxAttempts {
			break
		}

		backoff := e.backoffFor(attempt)
		if err := e.sleeper(ctx, backoff); err != nil {
			return nil, attempt, types.NewGatewayError(req.ID, types.ReasonBackendTimeout,
				"retry backoff canceled: %v", err)
		}
	}

	exhausted := types.NewGatewayError(req.ID, types.ReasonRetryExhausted,
		"retry exhausted after %d/%d attempts, last reason=%s: %s",
		e.policy.MaxAttempts, e.policy.MaxAttempts, reasonOf(lastErr), messageOf(lastErr))
	return nil, e.policy.MaxAttempts, exhausted
}

// backoffFor 计算第 attempt 次失败后（即第 attempt+1 次尝试前）的退避时长。
func (e *Executor) backoffFor(attempt int) time.Duration {
	d := float64(e.policy.InitialBackoff) * math.Pow(2, float64(attempt-1))
	backoff := time.Duration(d)
	if e.policy.MaxBackoff > 0 && backoff > e.policy.MaxBackoff {
		backoff = e.policy.MaxBackoff
	}
	if backoff < 0 { // 溢出保护
		backoff = e.policy.MaxBackoff
	}
	return backoff
}

func reasonOf(err error) types.Reason {
	if ge, ok := err.(*types.GatewayError); ok {
		return ge.Reason
	}
	return "non_gateway_error"
}

func messageOf(err error) string {
	if ge, ok := err.(*types.GatewayError); ok {
		return ge.Message
	}
	return err.Error()
}
