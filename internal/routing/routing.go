package routing

import (
	"context"
	"fmt"
	"sync"

	"github.com/h3yow/model-inference-gateway-go/internal/types"
)

// BackendRef 描述一个模型版本注册到哪个后端、当前是否在线。
type BackendRef struct {
	BackendName string
	Version     string
	Online      bool
	Basis       string // 路由判定依据，供日志/排障使用
}

type versionEntry struct {
	backendName string
	online      bool
}

// CanaryRule 描述同一模型两个版本之间的灰度比例。
type CanaryRule struct {
	Model     string // 模型名
	Stable    string // 稳定版本
	Canary    string // 灰度版本
	CanaryPct int    // 灰度版本流量百分比，取值 [0,100]
}

// Table 维护模型版本注册表与灰度规则，所有读写均加锁。
type Table struct {
	mu       sync.RWMutex
	models   map[string]map[string]*versionEntry // model -> version -> entry
	canaries map[string]CanaryRule               // model -> 生效中的灰度规则
}

// NewTable 创建空路由表。
func NewTable() *Table {
	return &Table{
		models:   make(map[string]map[string]*versionEntry),
		canaries: make(map[string]CanaryRule),
	}
}

// Register 注册（或更新）模型版本到后端的绑定及其在线状态。
func (t *Table) Register(model, version, backendName string, online bool) error {
	if model == "" || version == "" || backendName == "" {
		return fmt.Errorf("register: model, version and backendName must all be non-empty")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	versions := t.models[model]
	if versions == nil {
		versions = make(map[string]*versionEntry)
		t.models[model] = versions
	}
	versions[version] = &versionEntry{backendName: backendName, online: online}
	return nil
}

// SetOffline 把版本置为下线；路由到下线版本会得到 version_offline。
func (t *Table) SetOffline(model, version string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	versions := t.models[model]
	if versions == nil || versions[version] == nil {
		return fmt.Errorf("set offline: model %q version %q not registered", model, version)
	}
	versions[version].online = false
	return nil
}

// SetOnline 把版本重新置为上线。
func (t *Table) SetOnline(model, version string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	versions := t.models[model]
	if versions == nil || versions[version] == nil {
		return fmt.Errorf("set online: model %q version %q not registered", model, version)
	}
	versions[version].online = true
	return nil
}

// SetCanary 配置灰度规则。
// 任何冲突都返回 canary_conflict，禁止写入：比例非法、两版本相同、
// 模型/版本未注册、任一版本已下线。
func (t *Table) SetCanary(rule CanaryRule) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if rule.CanaryPct < 0 || rule.CanaryPct > 100 {
		return types.NewGatewayError("", types.ReasonCanaryConflict,
			"canary percentage %d out of range [0,100] for model %q", rule.CanaryPct, rule.Model)
	}
	if rule.Stable == rule.Canary {
		return types.NewGatewayError("", types.ReasonCanaryConflict,
			"stable and canary version must differ for model %q (both %q)", rule.Model, rule.Stable)
	}
	versions := t.models[rule.Model]
	if versions == nil {
		return types.NewGatewayError("", types.ReasonCanaryConflict,
			"model %q has no registered versions", rule.Model)
	}
	for _, v := range []string{rule.Stable, rule.Canary} {
		e := versions[v]
		if e == nil {
			return types.NewGatewayError("", types.ReasonCanaryConflict,
				"canary references unregistered version %q of model %q", v, rule.Model)
		}
		if !e.online {
			return types.NewGatewayError("", types.ReasonCanaryConflict,
				"canary references offline version %q of model %q", v, rule.Model)
		}
	}
	t.canaries[rule.Model] = rule
	return nil
}

// ClearCanary 移除模型的灰度规则（灰度结束）。
func (t *Table) ClearCanary(model string) {
	t.mu.Lock()
	delete(t.canaries, model)
	t.mu.Unlock()
}

// Route 决定请求实际命中的版本。绝不静默回落：
//   - 显式指定版本：未注册 -> version_unregistered；已下线 -> version_offline。
//   - 未指定版本（"任意可用版本"）：
//     有灰度规则时按 hashKey 确定性分流；无灰度规则时要求模型恰好只有一个在线版本，
//     否则返回 canary_conflict（无法安全决定默认版本）。
func (t *Table) Route(ctx context.Context, req *types.Request, hashKey uint64) (BackendRef, error) {
	_ = ctx
	t.mu.RLock()
	defer t.mu.RUnlock()

	versions := t.models[req.Model]
	if versions == nil {
		return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonVersionUnregistered,
			"model %q is not registered", req.Model)
	}

	// 显式版本：直接、严格地匹配。
	if req.Version != "" {
		e := versions[req.Version]
		if e == nil {
			return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonVersionUnregistered,
				"model %q version %q is not registered", req.Model, req.Version)
		}
		if !e.online {
			return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonVersionOffline,
				"model %q version %q is offline", req.Model, req.Version)
		}
		return BackendRef{
			BackendName: e.backendName, Version: req.Version, Online: true,
			Basis: fmt.Sprintf("显式指定版本 %s", req.Version),
		}, nil
	}

	// 未指定版本：优先灰度规则。
	rule, hasCanary := t.canaries[req.Model]
	if hasCanary {
		chosen := rule.Stable
		basis := "落在稳定版本区间"
		// hashKey%100 in [0,99]；[0,CanaryPct) -> 灰度版本
		if hashKey%100 < uint64(rule.CanaryPct) {
			chosen = rule.Canary
			basis = "落在灰度版本区间"
		}
		e := versions[chosen]
		// SetCanary 已保证两版本在线；这里防御性检查，绝不静默落到"默认版本"。
		if e == nil || !e.online {
			return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonCanaryConflict,
				"canary-selected version %q is not routable for model %q", chosen, req.Model)
		}
		return BackendRef{
			BackendName: e.backendName, Version: chosen, Online: true,
			Basis: fmt.Sprintf("灰度规则 stable=%s canary=%s pct=%d%%, hash=%d, %s",
				rule.Stable, rule.Canary, rule.CanaryPct, hashKey%100, basis),
		}, nil
	}

	// 无灰度规则：仅当模型恰有一个在线版本时才允许隐式路由。
	var onlineVersions []string
	for v, e := range versions {
		if e.online {
			onlineVersions = append(onlineVersions, v)
		}
	}
	if len(onlineVersions) == 0 {
		return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonVersionOffline,
			"model %q has no online version", req.Model)
	}
	if len(onlineVersions) > 1 {
		return BackendRef{}, types.NewGatewayError(req.ID, types.ReasonCanaryConflict,
			"model %q has %d online versions but no canary rule; refusing to pick a silent default",
			req.Model, len(onlineVersions))
	}
	v := onlineVersions[0]
	e := versions[v]
	return BackendRef{
		BackendName: e.backendName, Version: v, Online: true,
		Basis: "无灰度规则且仅一个在线版本，唯一可选",
	}, nil
}
