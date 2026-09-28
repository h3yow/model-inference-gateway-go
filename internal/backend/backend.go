package backend

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// Backend 是推理后端的统一接口（真实环境可替换为 HTTP/gRPC 客户端）。
type Backend interface {
	Name() string
	Infer(ctx context.Context, req *types.Request) (*types.Response, error)
}

// FailureMode 描述模拟后端的故障模式。
type FailureMode int32

const (
	ModeOK       FailureMode = iota // 正常
	ModeOverload                    // 后端过载（可重试）
	ModeTimeout                     // 后端超时（可重试）
	ModePartial                     // 部分失败：返回了损坏结果（非可重试）
)

func (m FailureMode) String() string {
	switch m {
	case ModeOK:
		return "ok"
	case ModeOverload:
		return "overload"
	case ModeTimeout:
		return "timeout"
	case ModePartial:
		return "partial_failure"
	default:
		return "unknown"
	}
}

// Simulated 是可编程的本地模拟推理后端。
type Simulated struct {
	name      string
	version   string
	mode      atomic.Int32
	delay     time.Duration
	callCount atomic.Int64
}

// NewSimulated 创建模拟后端。
// name 后端名称；version 服务版本；mode 初始故障模式；delay 处理耗时（超时模式下会睡满）。
func NewSimulated(name, version string, mode FailureMode, delay time.Duration) *Simulated {
	s := &Simulated{name: name, version: version, delay: delay}
	s.mode.Store(int32(mode))
	return s
}

// SetFailureMode 动态调整故障模式（测试中用于制造超载/灰度切换/故障恢复）。
func (s *Simulated) SetFailureMode(mode FailureMode) {
	s.mode.Store(int32(mode))
}

// CallCount 返回累计被调用次数（观测重试与降级行为）。
func (s *Simulated) CallCount() int64 { return s.callCount.Load() }

func (s *Simulated) Name() string { return s.name }

// Version 返回该后端固定服务的模型版本。
func (s *Simulated) Version() string { return s.version }

func (s *Simulated) Infer(ctx context.Context, req *types.Request) (*types.Response, error) {
	s.callCount.Add(1)
	mode := FailureMode(s.mode.Load())

	if err := ctx.Err(); err != nil {
		return nil, types.NewGatewayError(req.ID, types.ReasonBackendTimeout,
			"backend %q canceled before inference: %v", s.name, err)
	}

	// 过载：立即拒绝，模拟后端线程池打满。
	if mode == ModeOverload {
		return nil, types.NewGatewayError(req.ID, types.ReasonBackendOverload,
			"backend %q is overloaded (thread pool saturated)", s.name)
	}

	// 超时/正常都需要等待 delay，期间尊重 ctx。
	if s.delay > 0 {
		timer := time.NewTimer(s.delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, types.NewGatewayError(req.ID, types.ReasonBackendTimeout,
				"backend %q timed out / canceled after %s: %v", s.name, s.delay, ctx.Err())
		case <-timer.C:
		}
	}

	switch mode {
	case ModeTimeout:
		return nil, types.NewGatewayError(req.ID, types.ReasonBackendTimeout,
			"backend %q inference timed out after %s", s.name, s.delay)
	case ModePartial:
		// 部分失败属于业务性失败，不假定可安全重试。
		return nil, types.NewGatewayError(req.ID, types.ReasonBackendFailed,
			"backend %q returned a partial/corrupt result (non-retryable)", s.name)
	}

	return &types.Response{
		RequestID: req.ID,
		Model:     req.Model,
		Version:   s.version,
		Source:    s.name,
		Output:    fmt.Sprintf("inference by %s[%s] for payload=%q", s.name, s.version, req.Payload),
	}, nil
}
