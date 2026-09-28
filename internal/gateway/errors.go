package gateway

import (
	"errors"
	"fmt"
)

// RejectReason 是所有拒绝/失败原因的机器可读分类，
// 调用方可据此区分限流、过载、路由、后端异常等不同情况。
// 各原因互斥：一次请求只会以其中一种原因失败。
type RejectReason string

const (
	ReasonNone RejectReason = ""

	// ---- 配额层（请求未到达路由）----
	ReasonRateLimited     RejectReason = "rate_limited"      // 超出调用方 QPS 配额（令牌桶空）
	ReasonTooManyInflight RejectReason = "too_many_inflight" // 超出调用方并发上限（并发槽满）

	// ---- 路由层 ----
	ReasonUnknownModel   RejectReason = "unknown_model"   // 模型完全未注册
	ReasonUnknownVersion RejectReason = "unknown_version" // 显式请求的版本未注册
	ReasonVersionOffline RejectReason = "version_offline" // 显式请求的版本已下线
	ReasonRouteConflict  RejectReason = "route_conflict"  // 灰度规则与注册表冲突/权重非法
	ReasonNoCandidate    RejectReason = "no_candidate"    // 灰度规则未指向任何可服务版本

	// ---- 后端层 ----
	ReasonBackendTimeout  RejectReason = "backend_timeout"  // 后端超时（可重试分类）
	ReasonBackendOverload RejectReason = "backend_overload" // 后端过载（可重试分类）
	ReasonBackendError    RejectReason = "backend_error"    // 后端永久性/其他错误（不可重试）

	// ---- 重试/降级层 ----
	ReasonNonRetryable     RejectReason = "non_retryable"     // 幂等性或失败分类不允许重试
	ReasonRetriesExhausted RejectReason = "retries_exhausted" // 可重试但尝试次数耗尽，随后降级
)

// GatewayError 携带可区分的失败原因，并可选包装底层错误。
type GatewayError struct {
	Reason  RejectReason
	Message string
	Cause   error
}

func (e *GatewayError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Reason, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

func (e *GatewayError) Unwrap() error { return e.Cause }

// NewGatewayError 构造一个带原因的网关错误。
func NewGatewayError(reason RejectReason, format string, args ...any) *GatewayError {
	return &GatewayError{
		Reason:  reason,
		Message: fmt.Sprintf(format, args...),
	}
}

// WrapGatewayError 构造一个包装底层错误的网关错误。
func WrapGatewayError(reason RejectReason, cause error, format string, args ...any) *GatewayError {
	return &GatewayError{
		Reason:  reason,
		Message: fmt.Sprintf(format, args...),
		Cause:   cause,
	}
}

// ReasonOf 提取错误中的 RejectReason；非网关错误返回 ReasonNone。
// 同时识别 *BackendError，将其映射到对应的后端层原因。
func ReasonOf(err error) RejectReason {
	if err == nil {
		return ReasonNone
	}
	var ge *GatewayError
	if errors.As(err, &ge) {
		return ge.Reason
	}
	var be *BackendError
	if errors.As(err, &be) {
		return be.mapReason()
	}
	return ReasonBackendError
}

// String 返回原因的字符串形式（空原因返回 "none"）。
func (r RejectReason) String() string {
	if r == ReasonNone {
		return "none"
	}
	return string(r)
}
