package gateway

import (
	"context"
	"fmt"
)

// FailureKind 对后端失败进行分类，决定是否可重试。
type FailureKind int

const (
	FailureNone      FailureKind = 0
	FailureOverload  FailureKind = 1 // 后端过载（429/503 语义）：可重试
	FailureTimeout   FailureKind = 2 // 后端超时：可重试
	FailureTransient FailureKind = 3 // 瞬时错误（断连等）：可重试
	FailurePermanent FailureKind = 4 // 永久失败（参数/404 等）：不可重试
)

// Retryable 报告该失败分类是否允许重试（仍需请求幂等）。
func (k FailureKind) Retryable() bool {
	switch k {
	case FailureOverload, FailureTimeout, FailureTransient:
		return true
	default:
		return false
	}
}

// mapReason 把后端失败分类映射为对外可区分的 RejectReason。
func (k FailureKind) mapReason() RejectReason {
	switch k {
	case FailureTimeout:
		return ReasonBackendTimeout
	case FailureOverload:
		return ReasonBackendOverload
	default:
		// FailureTransient 在首次失败时也归入后端错误；
		// 若重试耗尽，编排层会改写为 retries_exhausted。
		return ReasonBackendError
	}
}

func (k FailureKind) String() string {
	switch k {
	case FailureOverload:
		return "overload"
	case FailureTimeout:
		return "timeout"
	case FailureTransient:
		return "transient"
	case FailurePermanent:
		return "permanent"
	default:
		return "none"
	}
}

// BackendError 携带失败分类，供重试策略判定。
type BackendError struct {
	Kind    FailureKind
	Message string
}

func (e *BackendError) Error() string {
	return fmt.Sprintf("backend %s: %s", e.Kind, e.Message)
}

func (e *BackendError) mapReason() RejectReason { return e.Kind.mapReason() }

// InferenceRequest 发往后端的推理请求。
type InferenceRequest struct {
	RequestID  string
	Model      string
	Version    string
	Caller     string
	Idempotent bool // 该请求是否幂等（只有幂等才允许重试）
	Payload    string
}

// InferenceResponse 后端返回的推理结果。
type InferenceResponse struct {
	RequestID    string
	Model        string
	Version      string
	Degraded     bool   // 是否为降级结果；正常后端响应恒为 false
	FallbackNote string // 降级说明（Degraded=true 时非空），便于调用方识别降级来源
	Output       string
}

// Backend 模拟推理后端的抽象。
type Backend interface {
	Infer(ctx context.Context, req *InferenceRequest) (*InferenceResponse, error)
}
