package gateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ScriptedOutcome 是按调用序号脚本化的模拟结果。
type ScriptedOutcome struct {
	Kind    FailureKind // FailureNone 表示成功
	Message string
	Latency time.Duration // 模拟处理耗时（仍会尊重 ctx 超时）
	Output  string
}

// ScriptedBackend 按调用次序回放预设结果：第 1 次调用用 outcomes[0]，依次类推；
// 超出脚本长度后重复最后一个结果。用于在测试中确定性制造
// "前几次过载/超时、随后成功"的部分失败链路。
type ScriptedBackend struct {
	model    string
	version  string
	outcomes []ScriptedOutcome

	mu    sync.Mutex
	calls int32
	hooks []func(req *InferenceRequest, attemptCall int)
}

// NewScriptedBackend 创建脚本化后端。
func NewScriptedBackend(model, version string, outcomes ...ScriptedOutcome) *ScriptedBackend {
	if len(outcomes) == 0 {
		outcomes = []ScriptedOutcome{{Kind: FailureNone, Output: "ok"}}
	}
	return &ScriptedBackend{model: model, version: version, outcomes: outcomes}
}

// OnCall 注册观察回调（如测试日志），每次调用触发一次。
func (b *ScriptedBackend) OnCall(fn func(req *InferenceRequest, attemptCall int)) {
	b.mu.Lock()
	b.hooks = append(b.hooks, fn)
	b.mu.Unlock()
}

// Calls 返回该后端累计被调用次数（含重试产生的调用）。
func (b *ScriptedBackend) Calls() int { return int(atomic.LoadInt32(&b.calls)) }

func (b *ScriptedBackend) Infer(ctx context.Context, req *InferenceRequest) (*InferenceResponse, error) {
	n := int(atomic.AddInt32(&b.calls, 1))
	b.mu.Lock()
	hooks := append([]func(*InferenceRequest, int){}, b.hooks...)
	b.mu.Unlock()
	for _, h := range hooks {
		h(req, n)
	}

	idx := n - 1
	if idx >= len(b.outcomes) {
		idx = len(b.outcomes) - 1
	}
	o := b.outcomes[idx]

	if o.Latency > 0 {
		select {
		case <-time.After(o.Latency):
		case <-ctx.Done():
			return nil, &BackendError{
				Kind:    FailureTimeout,
				Message: fmt.Sprintf("model %s/%s: simulated latency %s interrupted: %v", b.model, b.version, o.Latency, ctx.Err()),
			}
		}
	}
	if o.Kind != FailureNone {
		msg := o.Message
		if msg == "" {
			msg = fmt.Sprintf("scripted %s failure on call #%d", o.Kind, n)
		}
		return nil, &BackendError{Kind: o.Kind, Message: msg}
	}
	out := o.Output
	if out == "" {
		out = fmt.Sprintf("output from %s/%s (call #%d, request=%s)", b.model, b.version, n, req.RequestID)
	}
	return &InferenceResponse{Model: b.model, Version: b.version, Degraded: false, Output: out}, nil
}

// FlakyBackend 持续返回某类失败，直到成功调用次数达到阈值后转为成功。
// succeedAfter=0 表示始终失败；succeedAfter=2 表示第 1、2 次失败，之后成功。
type FlakyBackend struct {
	model        string
	version      string
	kind         FailureKind
	succeedAfter int64

	failures int64
	calls    int64
}

// NewFlakyBackend 创建不稳定后端。
func NewFlakyBackend(model, version string, kind FailureKind, succeedAfter int) *FlakyBackend {
	return &FlakyBackend{model: model, version: version, kind: kind, succeedAfter: int64(succeedAfter)}
}

// Failures 返回累计失败注入次数。
func (b *FlakyBackend) Failures() int { return int(atomic.LoadInt64(&b.failures)) }

// CallsCount 返回该后端累计被调用次数（含重试产生的调用）。
func (b *FlakyBackend) CallsCount() int { return int(atomic.LoadInt64(&b.calls)) }

func (b *FlakyBackend) Infer(ctx context.Context, req *InferenceRequest) (*InferenceResponse, error) {
	select {
	case <-ctx.Done():
		return nil, &BackendError{Kind: FailureTimeout, Message: ctx.Err().Error()}
	default:
	}
	atomic.AddInt64(&b.calls, 1)
	failNo := atomic.AddInt64(&b.failures, 1)
	if failNo <= b.succeedAfter {
		return nil, &BackendError{Kind: b.kind, Message: fmt.Sprintf("injected %s failure", b.kind)}
	}
	// 已度过失败窗口：失败计数回退，使 Failures 只统计真正注入的失败。
	atomic.AddInt64(&b.failures, -1)
	return &InferenceResponse{
		Model:    b.model,
		Version:  b.version,
		Degraded: false,
		Output:   fmt.Sprintf("output from %s/%s (request=%s)", b.model, b.version, req.RequestID),
	}, nil
}
