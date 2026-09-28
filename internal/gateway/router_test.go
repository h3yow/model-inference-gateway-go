package gateway

import (
	"context"
	"fmt"
	"testing"
)

// stubBackend 记录命中次数的最简后端。
type stubBackend struct {
	name string
	hits chan string
}

func (b *stubBackend) Infer(ctx context.Context, req *InferenceRequest) (*InferenceResponse, error) {
	if b.hits != nil {
		b.hits <- b.name
	}
	return &InferenceResponse{Model: req.Model, Version: req.Version, Output: b.name}, nil
}

func setupTwoVersions(t *testing.T) (*Router, *stubBackend, *stubBackend) {
	t.Helper()
	r := NewRouter()
	v1 := &stubBackend{name: "v1"}
	v2 := &stubBackend{name: "v2"}
	if err := r.RegisterVersion(VersionInfo{Model: "m", Version: "v1", State: VersionActive, Backend: v1}); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterVersion(VersionInfo{Model: "m", Version: "v2", State: VersionCanary, Backend: v2}); err != nil {
		t.Fatal(err)
	}
	return r, v1, v2
}

func TestRoutePinnedVersions(t *testing.T) {
	r, v1, v2 := setupTwoVersions(t)
	if err := r.SetRule(RouteRule{Model: "m", Stable: "v1", StableWeight: 100}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	d1, err := r.Resolve(ctx, RouteRequest{Model: "m", Version: "v2", ID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if d1.Version != "v2" || !d1.Pinned || d1.Backend != v2.backendRef() {
		t.Fatalf("pin v2 decision wrong: %+v", d1)
	}
	t.Logf("[r1] 显式指定 v2 -> 命中 v2 (pinned=%v)", d1.Pinned)

	d2, err := r.Resolve(ctx, RouteRequest{Model: "m", Version: "v1", ID: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	if d2.Version != "v1" || d2.Backend != v1.backendRef() {
		t.Fatalf("pin v1 decision wrong: %+v", d2)
	}
	t.Logf("[r2] 显式指定 v1 -> 命中 v1 (pinned=%v)", d2.Pinned)
}

func TestRouteUnknownAndOffline(t *testing.T) {
	r, _, _ := setupTwoVersions(t)
	ctx := context.Background()

	_, err := r.Resolve(ctx, RouteRequest{Model: "nope", ID: "x"})
	assertReason(t, "x", err, ReasonUnknownModel)

	_, err = r.Resolve(ctx, RouteRequest{Model: "m", Version: "v9", ID: "y"})
	assertReason(t, "y", err, ReasonUnknownVersion)

	// 未配置灰度规则且未 pin 版本：不能静默落到默认版本。
	_, err = r.Resolve(ctx, RouteRequest{Model: "m", ID: "z"})
	assertReason(t, "z", err, ReasonNoCandidate)

	if err := r.SetVersionState("m", "v2", VersionOffline); err != nil {
		t.Fatal(err)
	}
	_, err = r.Resolve(ctx, RouteRequest{Model: "m", Version: "v2", ID: "w"})
	assertReason(t, "w", err, ReasonVersionOffline)
	t.Log("已下线版本显式请求被拒绝，且未回落默认版本")
}

func TestRouteRuleConflicts(t *testing.T) {
	r, _, _ := setupTwoVersions(t)

	cases := []struct {
		name   string
		rule   RouteRule
		reason RejectReason
	}{
		{"权重和不为100", RouteRule{Model: "m", Stable: "v1", StableWeight: 70, Canaries: map[string]int{"v2": 20}}, ReasonRouteConflict},
		{"canary引用未注册版本", RouteRule{Model: "m", Stable: "v1", StableWeight: 50, Canaries: map[string]int{"vX": 50}}, ReasonUnknownVersion},
		{"stable与canary重复", RouteRule{Model: "m", Stable: "v1", StableWeight: 50, Canaries: map[string]int{"v1": 50}}, ReasonRouteConflict},
		{"全量canary超100", RouteRule{Model: "m", Canaries: map[string]int{"v2": 120}}, ReasonRouteConflict},
		{"未知模型", RouteRule{Model: "ghost", Stable: "v1", StableWeight: 100}, ReasonUnknownModel},
	}
	for _, c := range cases {
		err := r.SetRule(c.rule)
		if err == nil {
			t.Fatalf("%s: 期望冲突错误，却安装成功", c.name)
		}
		assertReason(t, "conflict-"+c.name, err, c.reason)
		t.Logf("灰度冲突[%s] 被拒绝: %v", c.name, err)
	}
}

func TestRouteOfflineVersionInvalidatesRule(t *testing.T) {
	r, _, _ := setupTwoVersions(t)
	if err := r.SetRule(RouteRule{Model: "m", Stable: "v1", StableWeight: 80, Canaries: map[string]int{"v2": 20}}); err != nil {
		t.Fatal(err)
	}
	// 滚动升级中途把 v1 下线：规则与现状冲突，灰度解析必须报错而非静默走 v2。
	if err := r.SetVersionState("m", "v1", VersionOffline); err != nil {
		t.Fatal(err)
	}
	_, err := r.Resolve(context.Background(), RouteRequest{Model: "m", ID: "roll"})
	assertReason(t, "roll", err, ReasonVersionOffline)
}

func TestRouteCanaryWeightDistribution(t *testing.T) {
	r, v1, v2 := setupTwoVersions(t)
	if err := r.SetRule(RouteRule{Model: "m", Stable: "v1", StableWeight: 80, Canaries: map[string]int{"v2": 20}}); err != nil {
		t.Fatal(err)
	}

	const n = 1000
	counts := map[string]int{"v1": 0, "v2": 0}
	stableForSameID := map[string]string{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("req-%d", i)
		d, err := r.Resolve(context.Background(), RouteRequest{Model: "m", ID: id})
		if err != nil {
			t.Fatal(err)
		}
		counts[d.Version]++
		stableForSameID[id] = d.Version
	}
	// 同一 ID 再次解析必须稳定命中同版本。
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("req-%d", i)
		d, _ := r.Resolve(context.Background(), RouteRequest{Model: "m", ID: id})
		if d.Version != stableForSameID[id] {
			t.Fatalf("灰度落点不稳定: %s 第一次=%s 第二次=%s", id, stableForSameID[id], d.Version)
		}
	}
	ratio := float64(counts["v2"]) / n
	t.Logf("灰度比例判定: v1=%d v2=%d, v2 占比=%.3f (期望约 0.20)", counts["v1"], counts["v2"], ratio)
	if ratio < 0.14 || ratio > 0.26 {
		t.Fatalf("灰度比例偏离过大: %.3f", ratio)
	}
	_ = v1
	_ = v2
}

func TestRouteRollingCanaryShift(t *testing.T) {
	r, _, _ := setupTwoVersions(t)
	mustT(t, r.SetRule(RouteRule{Model: "m", Stable: "v1", StableWeight: 80, Canaries: map[string]int{"v2": 20}}))

	sample := func(canaryWeightHint int) float64 {
		const n = 1000
		c2 := 0
		for i := 0; i < n; i++ {
			d, err := r.Resolve(context.Background(), RouteRequest{Model: "m", ID: fmt.Sprintf("roll-%d", i)})
			if err != nil {
				t.Fatal(err)
			}
			if d.Version == "v2" {
				c2++
			}
		}
		return float64(c2) / n
	}
	r1 := sample(20)
	mustT(t, r.SetRule(RouteRule{Model: "m", Stable: "v1", StableWeight: 50, Canaries: map[string]int{"v2": 50}}))
	r2 := sample(50)
	t.Logf("滚动升级灰度切换: v2 占比 %.3f -> %.3f (20%% -> 50%%)", r1, r2)
	if !(r1 < 0.30) || !(r2 > 0.40 && r2 < 0.60) {
		t.Fatalf("灰度切换未按权重生效: %.3f -> %.3f", r1, r2)
	}
}

func mustT(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (b *stubBackend) backendRef() Backend { return Backend(b) }
