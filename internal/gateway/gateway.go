package gateway

import (
	"context"
	"hash/fnv"
	"sync"

	"github.com/h3yow/model-inference-gateway-go/internal/backend"
	"github.com/h3yow/model-inference-gateway-go/internal/ratelimit"
	"github.com/h3yow/model-inference-gateway-go/internal/retry"
	"github.com/h3yow/model-inference-gateway-go/internal/routing"
	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// Logger 在处理链路各阶段打印请求标识与判定依据。
type Logger interface {
	Logf(requestID, stage, format string, args ...any)
}

// Gateway 组合限流、路由、重试与降级，是统一推理入口。
type Gateway struct {
	limiter  *ratelimit.Limiter
	table    *routing.Table
	backends map[string]backend.Backend
	executor *retry.Executor
	fallback backend.Backend
	logger   Logger

	// inflight 仅用于优雅关闭：等待所有在途请求结束。
	wg sync.WaitGroup
}

// Config 是 Gateway 的构造配置。
type Config struct {
	Limiter  *ratelimit.Limiter
	Table    *routing.Table
	Backends []backend.Backend
	Executor *retry.Executor
	Fallback backend.Backend // 可选；为 nil 则不做降级，直接透出错误
	Logger   Logger
}

// New 创建网关并完成依赖校验。
func New(cfg Config) (*Gateway, error) {
	if cfg.Limiter == nil {
		return nil, types.NewGatewayError("", types.ReasonNone, "gateway: limiter is required")
	}
	if cfg.Table == nil {
		return nil, types.NewGatewayError("", types.ReasonNone, "gateway: routing table is required")
	}
	if cfg.Executor == nil {
		return nil, types.NewGatewayError("", types.ReasonNone, "gateway: retry executor is required")
	}
	m := make(map[string]backend.Backend, len(cfg.Backends))
	for _, b := range cfg.Backends {
		m[b.Name()] = b
	}
	return &Gateway{
		limiter:  cfg.Limiter,
		table:    cfg.Table,
		backends: m,
		executor: cfg.Executor,
		fallback: cfg.Fallback,
		logger:   cfg.Logger,
	}, nil
}

// Infer 是统一推理入口。
//
// 处理顺序（每一步失败都有独立、可区分的原因，绝不静默）：
//  1. 调用方限流 + 并发上限；
//  2. 模型版本路由（显式严格匹配 / 灰度确定性分流）；
//  3. 在目标后端上执行带退避的有限重试（仅幂等+可重试失败）；
//  4. 仍失败时调用降级后端，降级成功的响应显式标记 Degraded=true 与降级说明；
//     降级后端也失败 -> fallback_failed。
func (g *Gateway) Infer(ctx context.Context, req *types.Request) (resp *types.Response, err error) {
	if req.ID == "" {
		return nil, types.NewGatewayError("", types.ReasonNone, "gateway: request ID is required")
	}
	g.wg.Add(1)
	defer g.wg.Done()

	if req.RequestedAt.IsZero() {
		req.RequestedAt = g.now()
	}

	// 1) 限流 / 并发
	release, rlErr := g.limiter.Acquire(ctx, req.Caller)
	if rlErr != nil {
		g.log(req.ID, "ratelimit", "拒绝: %v", rlErr)
		return nil, g.attachRequestID(rlErr, req.ID)
	}
	defer release()
	g.log(req.ID, "ratelimit", "放行 caller=%s", req.Caller)

	// 2) 路由
	hashKey := stableHash(req.ID, req.Caller, req.Model, req.Version)
	ref, routeErr := g.table.Route(ctx, req, hashKey)
	if routeErr != nil {
		g.log(req.ID, "routing", "拒绝: %v", routeErr)
		return nil, g.attachRequestID(routeErr, req.ID)
	}
	g.log(req.ID, "routing", "命中 model=%s version=%s backend=%s 依据=%s",
		req.Model, ref.Version, ref.BackendName, ref.Basis)

	// 3) 取后端 + 重试执行
	b, ok := g.backends[ref.BackendName]
	if !ok {
		return nil, types.NewGatewayError(req.ID, types.ReasonBackendNotFound,
			"routed backend %q (model=%s version=%s) is not registered in gateway",
			ref.BackendName, req.Model, ref.Version)
	}

	resp, attempts, execErr := g.executor.Do(ctx, b, req)
	if execErr == nil {
		resp.Attempts = attempts
		resp.RequestID = req.ID
		g.log(req.ID, "execute", "成功 backend=%s attempts=%d", ref.BackendName, attempts)
		return resp, nil
	}
	g.log(req.ID, "execute", "失败 backend=%s attempts=%d err=%v", ref.BackendName, attempts, execErr)

	// 4) 降级
	if g.fallback == nil {
		return nil, g.attachRequestID(execErr, req.ID)
	}
	g.log(req.ID, "fallback", "尝试降级后端=%s", g.fallback.Name())
	fbResp, fbErr := g.fallback.Infer(ctx, req)
	if fbErr != nil {
		g.log(req.ID, "fallback", "降级也失败: %v", fbErr)
		return nil, types.NewGatewayError(req.ID, types.ReasonFallbackFailed,
			"primary failed (%v); fallback %q failed: %v",
			execErr, g.fallback.Name(), fbErr)
	}

	// 显式标记这是降级结果，调用方不能把它当作正常结果。
	fbResp.RequestID = req.ID
	fbResp.Model = req.Model
	fbResp.Degraded = true
	fbResp.Source = g.fallback.Name()
	fbResp.DegradeNote = "primary backend failed after " +
		itoa(attempts) + " attempt(s): " + execErr.Error()
	fbResp.Attempts = attempts
	g.log(req.ID, "fallback", "降级成功 source=%s note=%q", g.fallback.Name(), fbResp.DegradeNote)
	return fbResp, nil
}

// Wait 阻塞直到所有在途请求完成（优雅关闭用）。
func (g *Gateway) Wait() { g.wg.Wait() }

func (g *Gateway) log(requestID, stage, format string, args ...any) {
	if g.logger != nil {
		g.logger.Logf(requestID, stage, format, args...)
	}
}

// attachRequestID 保证返回给调用方的错误都带上请求标识（限流器本身不知道请求 ID）。
func (g *Gateway) attachRequestID(err error, requestID string) error {
	if ge, ok := err.(*types.GatewayError); ok {
		if ge.RequestID == "" {
			ge.RequestID = requestID
		}
		return ge
	}
	return err
}

func stableHash(parts ...string) uint64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}
