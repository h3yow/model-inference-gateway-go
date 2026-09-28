package gateway

import (
	"context"
	"errors"
	"fmt"
)

// FallbackFunc 在所有后端尝试失败后产出降级结果。
type FallbackFunc func(ctx context.Context, req *InferenceRequest, cause error) (*InferenceResponse, error)

// Degrader 负责在后端失败后产出"调用方可明确识别"的降级结果。
type Degrader struct {
	fn FallbackFunc
}

// NewDegrader 创建降级器；fn 为 nil 时使用 DefaultFallback。
func NewDegrader(fn FallbackFunc) *Degrader {
	if fn == nil {
		fn = DefaultFallback
	}
	return &Degrader{fn: fn}
}

// Fallback 返回降级结果。契约：
//   - 成功降级时响应 Degraded 恒为 true，FallbackNote 非空并写明触发原因；
//   - 若 fallback 自身无法产出结果，则返回错误，编排层以原始 cause 的 Reason 失败。
func (d *Degrader) Fallback(ctx context.Context, req *InferenceRequest, cause error) (*InferenceResponse, error) {
	resp, err := d.fn(ctx, req, cause)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("degrade: fallback returned nil response")
	}
	// 强制标记，避免某个自定义 fallback 漏标导致调用方误判为正常结果。
	resp.Degraded = true
	if resp.FallbackNote == "" {
		resp.FallbackNote = "degraded response; cause=" + ReasonOf(cause).String()
	}
	return resp, nil
}

// DefaultFallback 默认降级实现：不报错，产出一个带明确标记的合成结果。
// note 同时写明网关层判定原因与底层后端分类（若有），
// 使调用方在降级结果上仍能区分"过载耗尽"与"超时耗尽"。
func DefaultFallback(ctx context.Context, req *InferenceRequest, cause error) (*InferenceResponse, error) {
	topReason := ReasonOf(cause)
	detail := ""
	var ge *GatewayError
	if errors.As(cause, &ge) && ge.Cause != nil {
		detail = ", last=" + ReasonOf(ge.Cause).String()
	}
	note := fmt.Sprintf("degraded: backend unavailable for %s/%s (request=%s, caller=%s): reason=%s%s",
		req.Model, req.Version, req.RequestID, req.Caller, topReason, detail)
	return &InferenceResponse{
		Model:        req.Model,
		Version:      req.Version,
		Degraded:     true,
		FallbackNote: note,
		Output:       "", // 降级结果不提供模型输出，防止被当作有效推理结论
	}, nil
}
