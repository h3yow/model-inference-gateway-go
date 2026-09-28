package types

import (
	"fmt"
	"time"
)

// Request 是上层业务对某个模型版本发起的推理请求。
type Request struct {
	ID          string    // 请求标识，贯穿日志与判定依据
	Caller      string    // 调用方标识，用于限流/并发配额
	Model       string    // 模型名称
	Version     string    // 请求指定的模型版本；空串表示"任意可用版本"
	Idempotent  bool      // 是否幂等（失败可重试的必要条件之一）
	Payload     string    // 推理输入（演示用字符串）
	RequestedAt time.Time // 请求发起时刻，用于日志
}

// Response 是一次推理的返回结果。
type Response struct {
	RequestID   string
	Model       string
	Version     string // 实际服务版本
	Degraded    bool   // 是否为降级结果
	Source      string // 结果来源：正常后端名称 / 降级后端名称
	Output      string
	DegradeNote string // 降级说明（仅 Degraded=true 时有意义）
	Attempts    int    // 实际尝试次数（含重试）
}

// Reason 是可区分的拒绝/失败原因分类。
type Reason string

const (
	ReasonNone                Reason = ""
	ReasonCallerNotConfigured Reason = "caller_not_configured"  // 调用方没有配额配置
	ReasonVersionUnregistered Reason = "version_unregistered"   // 请求了未注册版本
	ReasonVersionOffline      Reason = "version_offline"        // 版本已下线
	ReasonCanaryConflict      Reason = "canary_conflict"        // 灰度配置冲突
	ReasonRateLimited         Reason = "rate_limited"           // 触发调用方限流
	ReasonCallerConcurrency   Reason = "caller_concurrency"     // 触发调用方并发上限
	ReasonBackendOverload     Reason = "backend_overload"       // 后端过载
	ReasonBackendTimeout      Reason = "backend_timeout"        // 后端超时
	ReasonBackendFailed       Reason = "backend_failed"         // 后端非可重试失败
	ReasonRetryExhausted      Reason = "retry_exhausted"        // 重试次数耗尽
	ReasonFallbackFailed      Reason = "fallback_failed"        // 降级后端也失败
	ReasonBackendNotFound     Reason = "backend_not_configured" // 路由指向的后端未在网关注册
)

// GatewayError 是网关所有拒绝/失败的统一错误类型。
type GatewayError struct {
	RequestID string
	Reason    Reason
	Message   string
}

func (e *GatewayError) Error() string {
	if e == nil {
		return ""
	}
	if e.RequestID != "" {
		return "gateway error [" + string(e.Reason) + "] request=" + e.RequestID + ": " + e.Message
	}
	return "gateway error [" + string(e.Reason) + "]: " + e.Message
}

// Retryable 判断该错误是否属于可重试的失败类别。
func (e *GatewayError) Retryable() bool {
	switch e.Reason {
	case ReasonBackendOverload, ReasonBackendTimeout:
		return true
	default:
		return false
	}
}

func NewGatewayError(requestID string, reason Reason, format string, args ...any) *GatewayError {
	return &GatewayError{RequestID: requestID, Reason: reason, Message: fmt.Sprintf(format, args...)}
}
