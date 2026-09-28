package gateway

import (
	"context"
	"fmt"
	"time"
)

// Request 是网关统一入口请求。
type Request struct {
	RequestID  string // 请求标识；为空时网关生成
	Caller     string
	Model      string
	Version    string // 显式指定版本（pin）；为空走灰度规则
	Idempotent bool   // 是否幂等：只有幂等请求允许失败重试
	Payload    string
}

// Event 描述网关处理过程中的一个判定事件，供日志/测试断言判定依据。
type Event struct {
	RequestID string
	Stage     string // "quota" | "route" | "attempt" | "retry" | "degrade" | "success" | "reject"
	Message   string
	Reason    RejectReason
}

// EventHook 观察事件（不得阻塞）。
type EventHook func(Event)

// Gateway 编排配额、路由、重试与降级。
type Gateway struct {
	router   *Router
	quotas   *QuotaManager
	retrier  *Retrier
	degrader *Degrader

	requestTimeout time.Duration // 整个请求（含重试）的超时；0 表示沿用调用方 ctx
	hook           EventHook
	sleep          func(ctx context.Context, d time.Duration) error // 可替换以便测试
	idGen          func() string
}

// Option 配置 Gateway。
type Option func(*Gateway)

// WithRequestTimeout 设置整请求超时。
func WithRequestTimeout(d time.Duration) Option {
	return func(g *Gateway) { g.requestTimeout = d }
}

// WithEventHook 安装事件观察钩子。
func WithEventHook(h EventHook) Option {
	return func(g *Gateway) { g.hook = h }
}

// withSleep 仅用于测试：替换退避等待实现。
func withSleep(s func(ctx context.Context, d time.Duration) error) Option {
	return func(g *Gateway) { g.sleep = s }
}

// New 组装网关。
func New(router *Router, quotas *QuotaManager, retrier *Retrier, degrader *Degrader, opts ...Option) *Gateway {
	g := &Gateway{
		router:   router,
		quotas:   quotas,
		retrier:  retrier,
		degrader: degrader,
		sleep:    defaultSleep,
		idGen:    newIDGenerator(),
	}
	for _, o := range opts {
		o(g)
	}
	return g
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Gateway) emit(reqID, stage string, reason RejectReason, format string, args ...any) {
	if g.hook == nil {
		return
	}
	g.hook(Event{
		RequestID: reqID,
		Stage:     stage,
		Reason:    reason,
		Message:   fmt.Sprintf(format, args...),
	})
}

// Infer 统一推理入口，处理顺序：配额 -> 路由 -> 后端调用(指数退避重试) -> 降级。
//
// 失败语义（错误为 *GatewayError，Reason 互斥可区分）：
//   - rate_limited / too_many_inflight：配额层拒绝，不进入路由；
//   - unknown_model / unknown_version / version_offline / no_candidate / route_conflict：路由层拒绝；
//   - backend_error：永久性后端失败，不重试、不降级；
//   - backend_timeout：退避等待期间整体超时；
//
// 可重试链路（过载/超时/瞬时）：
//   - 请求幂等：按策略重试；次数耗尽后降级，降级响应 Degraded=true，FallbackNote 写明 retries_exhausted；
//   - 请求非幂等：不重试，直接降级，FallbackNote 写明 non_retryable。
func (g *Gateway) Infer(ctx context.Context, req *Request) (*InferenceResponse, error) {
	reqID := req.RequestID
	if reqID == "" {
		reqID = g.idGen()
	}

	if g.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, g.requestTimeout)
		defer cancel()
	}

	// 1) 配额：并发槽 + 令牌在同一临界区判定。
	release, err := g.quotas.Acquire(ctx, req.Caller)
	if err != nil {
		g.emit(reqID, "reject", ReasonOf(err), "quota denied for caller=%s: %v", req.Caller, err)
		return nil, err
	}
	defer release()
	g.emit(reqID, "quota", ReasonNone, "quota acquired for caller=%s", req.Caller)

	// 2) 路由：显式版本或灰度权重；任何异常都显式失败，不回落默认版本。
	dec, err := g.router.Resolve(ctx, RouteRequest{
		Model:   req.Model,
		Version: req.Version,
		ID:      reqID,
	})
	if err != nil {
		g.emit(reqID, "reject", ReasonOf(err), "route failed for model=%s version=%q: %v", req.Model, req.Version, err)
		return nil, err
	}
	g.emit(reqID, "route", ReasonNone,
		"resolved model=%s -> version=%s pinned=%v canary=%v", dec.Model, dec.Version, dec.Pinned, dec.Canary)

	ireq := &InferenceRequest{
		RequestID:  reqID,
		Model:      dec.Model,
		Version:    dec.Version,
		Caller:     req.Caller,
		Idempotent: req.Idempotent,
		Payload:    req.Payload,
	}

	// 3) 后端调用 + 重试。
	var lastErr error
	maxAttempts := g.retrier.MaxAttempts()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, callErr := dec.Backend.Infer(ctx, ireq)
		if callErr == nil {
			resp.RequestID = reqID
			g.emit(reqID, "success", ReasonNone,
				"backend %s/%s succeeded on attempt %d", dec.Model, dec.Version, attempt)
			return resp, nil
		}
		lastErr = callErr
		g.emit(reqID, "attempt", ReasonOf(callErr),
			"backend %s/%s attempt %d/%d failed: %v (idempotent=%v, retryable-kind=%v)",
			dec.Model, dec.Version, attempt, maxAttempts, callErr, ireq.Idempotent, IsRetryableError(callErr))

		// 永久性失败：立即失败，不重试、不降级。
		if !IsRetryableError(callErr) {
			ge := WrapGatewayError(ReasonOf(callErr), callErr,
				"permanent backend failure for %s/%s on attempt %d", dec.Model, dec.Version, attempt)
			g.emit(reqID, "reject", ge.Reason, "giving up: %v", ge)
			return nil, ge
		}

		// 可重试失败但请求非幂等：禁止重试，直接降级。
		if !ireq.Idempotent {
			ge := WrapGatewayError(ReasonNonRetryable, callErr,
				"request %s is not idempotent; retry suppressed for %s/%s", reqID, dec.Model, dec.Version)
			return g.doDegrade(ctx, ireq, ge, "non-idempotent", reqID)
		}

		// 幂等且仍有剩余次数：退避后重试。
		if attempt < maxAttempts {
			d := g.retrier.Backoff(attempt)
			g.emit(reqID, "retry", ReasonNone,
				"retrying %s/%s after %s (next attempt %d/%d)", dec.Model, dec.Version, d, attempt+1, maxAttempts)
			if err := g.sleep(ctx, d); err != nil {
				ge := WrapGatewayError(ReasonBackendTimeout, err,
					"backoff wait interrupted for %s/%s after attempt %d", dec.Model, dec.Version, attempt)
				// 退避被超时打断同样走降级，保留后端链路信息。
				return g.doDegrade(ctx, ireq, ge, "timeout-during-backoff", reqID)
			}
			continue
		}
	}

	// 4) 可重试失败但次数耗尽：降级，原因明确标记为 retries_exhausted。
	ge := WrapGatewayError(ReasonRetriesExhausted, lastErr,
		"all %d attempts exhausted for %s/%s (last error: %v)",
		maxAttempts, dec.Model, dec.Version, lastErr)
	return g.doDegrade(ctx, ireq, ge, "retries-exhausted", reqID)
}

// doDegrade 产出降级结果并发出事件；若降级器自身失败，则返回原始错误。
func (g *Gateway) doDegrade(ctx context.Context, ireq *InferenceRequest, cause *GatewayError, kind, reqID string) (*InferenceResponse, error) {
	g.emit(reqID, "degrade", cause.Reason, "entering degrade (%s): %v", kind, cause)
	resp, err := g.degrader.Fallback(ctx, ireq, cause)
	if err != nil {
		g.emit(reqID, "reject", cause.Reason, "degrade failed: %v", err)
		return nil, cause
	}
	g.emit(reqID, "degrade", cause.Reason,
		"degraded response produced: version=%s note=%q", resp.Version, resp.FallbackNote)
	resp.RequestID = reqID
	return resp, nil
}
