package gateway

import (
	"context"
	"testing"
)

func baseQuota() *QuotaManager {
	return NewQuotaManager(map[string]CallerQuota{
		"c":           {Caller: "c", RateLimitPerSec: 100000, Burst: 100000, MaxInflight: 100000},
		DefaultCaller: {Caller: DefaultCaller, RateLimitPerSec: 100000, Burst: 100000, MaxInflight: 100000},
	})
}

func registerModel(t *testing.T, r *Router, model string, b Backend) {
	t.Helper()
	if err := r.RegisterVersion(VersionInfo{Model: model, Version: "v1", State: VersionActive, Backend: b}); err != nil {
		t.Fatal(err)
	}
	if err := r.SetRule(RouteRule{Model: model, Stable: "v1", StableWeight: 100}); err != nil {
		t.Fatal(err)
	}
}

// 可重试失败 + 幂等请求：重试后成功，后端被调用 2 次，返回非降级结果。
func TestInferRetryThenSuccess(t *testing.T) {
	r := NewRouter()
	b := NewFlakyBackend("m", "v1", FailureOverload, 1) // 第 1 次过载，之后成功
	registerModel(t, r, "m", b)
	gw, events := newTestGateway(t, r, baseQuota(), 3)

	resp, err := gw.Infer(context.Background(), &Request{
		RequestID: "retry-ok", Caller: "c", Model: "m", Idempotent: true,
	})
	if err != nil {
		t.Fatalf("应在重试后成功: %v", err)
	}
	if resp.Degraded {
		t.Fatal("重试成功的结果不得标记为降级")
	}
	if b.Failures() != 1 {
		t.Fatalf("后端应恰好注入 1 次失败, got %d", b.Failures())
	}
	if b.CallsCount() != 2 {
		t.Fatalf("后端应被调用 2 次(首调+1次重试), got %d", b.CallsCount())
	}
	t.Logf("[retry-ok] 判定依据: 首次过载 -> 退避重试 -> 成功, 后端调用=%d, degraded=%v",
		b.CallsCount(), resp.Degraded)
	logEvents(t, "retry-ok", *events)
}

// 可重试失败 + 非幂等请求：禁止重试，直接降级，原因 non_retryable。
func TestInferNonIdempotentNoRetryDegrade(t *testing.T) {
	r := NewRouter()
	b := NewFlakyBackend("m", "v1", FailureTimeout, 99)
	registerModel(t, r, "m", b)
	gw, events := newTestGateway(t, r, baseQuota(), 5) // 即使给 5 次额度也不许试

	resp, err := gw.Infer(context.Background(), &Request{
		RequestID: "non-idemp", Caller: "c", Model: "m", Idempotent: false,
	})
	if err != nil {
		t.Fatalf("非幂等可重试失败应降级而非报错: %v", err)
	}
	if !resp.Degraded {
		t.Fatal("降级结果必须 Degraded=true")
	}
	if resp.FallbackNote == "" || !contains(resp.FallbackNote, "non_retryable") {
		t.Fatalf("FallbackNote 必须写明 non_retryable, got %q", resp.FallbackNote)
	}
	if b.CallsCount() != 1 {
		t.Fatalf("非幂等请求只允许调用后端 1 次, got %d", b.CallsCount())
	}
	t.Logf("[non-idemp] 判定依据: 超时但非幂等 -> 不重试 -> 降级, note=%q", resp.FallbackNote)
	logEvents(t, "non-idemp", *events)
}

// 可重试失败 + 幂等 + 持续失败：重试耗尽后降级，原因 retries_exhausted。
func TestInferRetriesExhaustedThenDegrade(t *testing.T) {
	r := NewRouter()
	b := NewFlakyBackend("m", "v1", FailureOverload, 999)
	registerModel(t, r, "m", b)
	gw, events := newTestGateway(t, r, baseQuota(), 3)

	resp, err := gw.Infer(context.Background(), &Request{
		RequestID: "exhausted", Caller: "c", Model: "m", Idempotent: true,
	})
	if err != nil {
		t.Fatalf("重试耗尽应降级而非报错: %v", err)
	}
	if !resp.Degraded {
		t.Fatal("必须标记降级")
	}
	if !contains(resp.FallbackNote, "retries_exhausted") {
		t.Fatalf("note 必须写明 retries_exhausted, got %q", resp.FallbackNote)
	}
	if b.CallsCount() != 3 {
		t.Fatalf("后端应被调用 3 次(达到上限), got %d", b.CallsCount())
	}
	t.Logf("[exhausted] 判定依据: 连续过载 -> 3 次尝试耗尽 -> 降级, note=%q", resp.FallbackNote)
	logEvents(t, "exhausted", *events)
}

// 永久失败：不重试、不降级，直接返回 backend_error。
func TestInferPermanentFailureHardFail(t *testing.T) {
	r := NewRouter()
	b := NewFlakyBackend("m", "v1", FailurePermanent, 999)
	registerModel(t, r, "m", b)
	gw, events := newTestGateway(t, r, baseQuota(), 5)

	resp, err := gw.Infer(context.Background(), &Request{
		RequestID: "perm", Caller: "c", Model: "m", Idempotent: true,
	})
	if err == nil {
		_ = resp
		t.Fatal("永久失败必须返回错误")
	}
	assertReason(t, "perm", err, ReasonBackendError)
	if resp != nil {
		t.Fatal("永久失败不得返回降级响应")
	}
	if b.CallsCount() != 1 {
		t.Fatalf("永久失败只能调用 1 次, got %d", b.CallsCount())
	}
	t.Log("[perm] 判定依据: permanent 分类不可重试 -> 直接硬失败 backend_error")
	logEvents(t, "perm", *events)
}

// 过载 vs 超时 vs 永久失败三种原因必须可区分。
func TestInferDistinguishableReasons(t *testing.T) {
	r := NewRouter()
	ov := NewFlakyBackend("m-ov", "v1", FailureOverload, 999)
	to := NewFlakyBackend("m-to", "v1", FailureTimeout, 999)
	pm := NewFlakyBackend("m-pm", "v1", FailurePermanent, 999)
	registerModel(t, r, "m-ov", ov)
	registerModel(t, r, "m-to", to)
	registerModel(t, r, "m-pm", pm)
	gw, _ := newTestGateway(t, r, baseQuota(), 1) // 只允许 1 次尝试，直接看到底层原因对应的降级/错误

	ovResp, ovErr := gw.Infer(context.Background(), &Request{RequestID: "d-ov", Caller: "c", Model: "m-ov", Idempotent: true})
	toResp, toErr := gw.Infer(context.Background(), &Request{RequestID: "d-to", Caller: "c", Model: "m-to", Idempotent: true})
	_, pmErr := gw.Infer(context.Background(), &Request{RequestID: "d-pm", Caller: "c", Model: "m-pm", Idempotent: true})

	if ovErr != nil || !ovResp.Degraded || !contains(ovResp.FallbackNote, "retries_exhausted") {
		t.Fatalf("过载链路异常: resp=%+v err=%v", ovResp, ovErr)
	}
	if toErr != nil || !toResp.Degraded || !contains(toResp.FallbackNote, "retries_exhausted") {
		t.Fatalf("超时链路异常: resp=%+v err=%v", toResp, toErr)
	}
	// 降级 note 中保留底层分类，过载与超时仍可从 cause 链区分。
	if !contains(ovResp.FallbackNote, "backend_overload") {
		t.Fatalf("过载降级 note 缺少 backend_overload: %q", ovResp.FallbackNote)
	}
	if !contains(toResp.FallbackNote, "backend_timeout") {
		t.Fatalf("超时降级 note 缺少 backend_timeout: %q", toResp.FallbackNote)
	}
	assertReason(t, "d-pm", pmErr, ReasonBackendError)
	t.Logf("[d-ov] 降级 note=%q", ovResp.FallbackNote)
	t.Logf("[d-to] 降级 note=%q", toResp.FallbackNote)
	t.Log("[d-pm] 永久失败 reason=backend_error")
}

// 部分失败：脚本化后端前 2 次过载第 3 次成功，MaxAttempts=3 应刚好在第 3 次成功。
func TestInferPartialFailureRecover(t *testing.T) {
	r := NewRouter()
	b := NewScriptedBackend("m", "v1",
		ScriptedOutcome{Kind: FailureOverload},
		ScriptedOutcome{Kind: FailureOverload},
		ScriptedOutcome{Kind: FailureNone, Output: "recovered"},
	)
	registerModel(t, r, "m", b)
	gw, events := newTestGateway(t, r, baseQuota(), 3)

	resp, err := gw.Infer(context.Background(), &Request{
		RequestID: "partial", Caller: "c", Model: "m", Idempotent: true,
	})
	if err != nil {
		t.Fatalf("第 3 次应恢复成功: %v", err)
	}
	if resp.Degraded || resp.Output != "recovered" {
		t.Fatalf("恢复结果异常: %+v", resp)
	}
	if b.Calls() != 3 {
		t.Fatalf("应调用 3 次, got %d", b.Calls())
	}
	t.Logf("[partial] 判定依据: 过载->过载->成功(第3次), output=%q", resp.Output)
	logEvents(t, "partial", *events)
}

// 端到端串联：先触发并发上限拒绝，再验证另一个请求正常成功；两入口结论一致可区分。
func TestInferQuotaRejectThenSuccess(t *testing.T) {
	r := NewRouter()
	b := NewScriptedBackend("m", "v1", ScriptedOutcome{Kind: FailureNone})
	registerModel(t, r, "m", b)
	qm := NewQuotaManager(map[string]CallerQuota{
		"c": {Caller: "c", RateLimitPerSec: 100, Burst: 100, MaxInflight: 1},
	})
	gw, _ := newTestGateway(t, r, qm, 2)

	hold, err := qm.Acquire(context.Background(), "c")
	if err != nil {
		t.Fatal(err)
	}
	_, err = gw.Infer(context.Background(), &Request{RequestID: "busy", Caller: "c", Model: "m"})
	assertReason(t, "busy", err, ReasonTooManyInflight)
	hold()

	resp, err := gw.Infer(context.Background(), &Request{RequestID: "freed", Caller: "c", Model: "m"})
	if err != nil {
		t.Fatalf("槽释放后应成功: %v", err)
	}
	if resp.Degraded {
		t.Fatal("正常结果不得降级")
	}
	t.Log("[busy] 并发占满 -> too_many_inflight; [freed] 槽释放后成功，结论可区分")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
