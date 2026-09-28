package routing

import (
	"context"
	"testing"

	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

func req(id, model, version string) *types.Request {
	return &types.Request{ID: id, Caller: "tester", Model: model, Version: version}
}

func mustReason(t *testing.T, err error, want types.Reason) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with reason %s, got nil", want)
	}
	ge, ok := err.(*types.GatewayError)
	if !ok {
		t.Fatalf("expected *GatewayError, got %T: %v", err, err)
	}
	if ge.Reason != want {
		t.Fatalf("reason=%s, want %s (msg=%s)", ge.Reason, want, ge.Message)
	}
	t.Logf("请求%s 拒绝原因=%s msg=%q", ge.RequestID, ge.Reason, ge.Message)
}

func TestExplicitVersionRejections(t *testing.T) {
	tab := NewTable()
	if err := tab.Register("llm", "v1", "backend-v1", true); err != nil {
		t.Fatal(err)
	}
	if err := tab.Register("llm", "v2", "backend-v2", false); err != nil { // v2 已下线
		t.Fatal(err)
	}
	ctx := context.Background()

	// 未注册的模型
	_, err := tab.Route(ctx, req("r-unknown-model", "ghost", "v1"), 0)
	mustReason(t, err, types.ReasonVersionUnregistered)

	// 未注册的版本
	_, err = tab.Route(ctx, req("r-unknown-ver", "llm", "v9"), 0)
	mustReason(t, err, types.ReasonVersionUnregistered)

	// 已下线版本
	_, err = tab.Route(ctx, req("r-offline", "llm", "v2"), 0)
	mustReason(t, err, types.ReasonVersionOffline)

	// 正常显式版本
	ref, err := tab.Route(ctx, req("r-ok", "llm", "v1"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if ref.BackendName != "backend-v1" || ref.Version != "v1" {
		t.Fatalf("unexpected ref: %+v", ref)
	}
	t.Logf("请求r-ok 判定依据=%s", ref.Basis)
}

func TestCanaryConflictRules(t *testing.T) {
	tab := NewTable()
	_ = tab.Register("llm", "v1", "b1", true)
	_ = tab.Register("llm", "v2", "b2", true)

	// 比例越界
	mustReason(t, tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 101}),
		types.ReasonCanaryConflict)
	// 两版本相同
	mustReason(t, tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v1", CanaryPct: 10}),
		types.ReasonCanaryConflict)
	// 引用未注册版本
	mustReason(t, tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "vX", CanaryPct: 10}),
		types.ReasonCanaryConflict)
	// 引用已下线版本
	_ = tab.SetOffline("llm", "v2")
	mustReason(t, tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 10}),
		types.ReasonCanaryConflict)

	// 冲突时规则不得生效：未指定版本且有两个在线版本(v1在线,v2下线=>仅一个在线) —— v2下线后应唯一路由到v1
	_ = tab.SetOnline("llm", "v2")
	// 上面所有 SetCanary 都失败了，模型有两个在线版本且无灰度规则 -> 必须拒绝而非静默选默认
	_, err := tab.Route(context.Background(), req("r-no-rule", "llm", ""), 0)
	mustReason(t, err, types.ReasonCanaryConflict)
}

func TestCanaryDeterministicSplitAndSwitch(t *testing.T) {
	tab := NewTable()
	_ = tab.Register("llm", "v1", "b1", true)
	_ = tab.Register("llm", "v2", "b2", true)

	routeVersion := func(hash uint64) string {
		ref, err := tab.Route(context.Background(), req("r-canary", "llm", ""), hash)
		if err != nil {
			t.Fatal(err)
		}
		return ref.Version
	}

	// 灰度 20%：hash 0..99 中 0..19 -> v2
	if err := tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 20}); err != nil {
		t.Fatal(err)
	}
	countV2 := 0
	for h := uint64(0); h < 100; h++ {
		v := routeVersion(h)
		if v == "v2" {
			countV2++
		}
		// 确定性：同一 hash 重复路由必须一致
		if routeVersion(h) != v {
			t.Fatalf("routing not deterministic for hash %d", h)
		}
	}
	if countV2 != 20 {
		t.Fatalf("canary share=%d, want 20", countV2)
	}
	t.Logf("判定依据: CanaryPct=20, hash[0,99]命中v2的数量=%d (20%%)", countV2)

	// 边界断言
	if routeVersion(19) != "v2" || routeVersion(20) != "v1" {
		t.Fatal("canary boundary must be [0,20)")
	}

	// 灰度切换到 80%：无需重启，立即生效
	if err := tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 80}); err != nil {
		t.Fatal(err)
	}
	countV2 = 0
	for h := uint64(0); h < 100; h++ {
		if routeVersion(h) == "v2" {
			countV2++
		}
	}
	if countV2 != 80 {
		t.Fatalf("after switch canary share=%d, want 80", countV2)
	}
	t.Logf("判定依据: 滚动升级切到CanaryPct=80, 命中v2的数量=%d", countV2)

	// 灰度 100%：全部到新版本；0%：全部到旧版本
	_ = tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 100})
	if routeVersion(99) != "v2" {
		t.Fatal("pct=100 must route everything to canary")
	}
	_ = tab.SetCanary(CanaryRule{Model: "llm", Stable: "v1", Canary: "v2", CanaryPct: 0})
	if routeVersion(0) != "v1" {
		t.Fatal("pct=0 must route everything to stable")
	}
}
