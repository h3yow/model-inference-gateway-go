package gateway

import (
	"context"
	"hash/fnv"
	"sort"
	"sync"
)

// VersionState 版本生命周期状态。
type VersionState string

const (
	VersionActive  VersionState = "active"  // 稳定承载流量
	VersionCanary  VersionState = "canary"  // 灰度中，按权重分流
	VersionOffline VersionState = "offline" // 已下线，显式指定也拒绝
)

// servable 报告该状态是否可承接流量。
func (s VersionState) servable() bool { return s == VersionActive || s == VersionCanary }

// VersionInfo 描述模型的一个已注册版本。
type VersionInfo struct {
	Model   string
	Version string
	State   VersionState
	Backend Backend
}

// RouteRule 描述一个模型的路由/灰度规则。
// 权重单位为百分比，StableWeight + ΣCanaries 必须等于 100。
// StableWeight < 0 表示自动取 100 - ΣCanaries（要求 ΣCanaries <= 100）。
type RouteRule struct {
	Model        string
	Stable       string         // stable 版本（可为空，表示全量灰度中）
	StableWeight int            // stable 权重；<0 自动计算
	Canaries     map[string]int // canary 版本 -> 权重
}

// RouteRequest 是一次路由解析的输入。
type RouteRequest struct {
	Model   string
	Version string // 显式指定版本（pin）；为空则按灰度规则路由
	ID      string // 请求标识，用于确定性灰度落点
}

// RouteDecision 是路由解析结果。
type RouteDecision struct {
	Model   string
	Version string
	Backend Backend
	Pinned  bool // 是否为显式版本指定
	Canary  bool // 是否命中灰度版本
}

// Router 维护版本注册表与灰度规则，并发安全。
type Router struct {
	mu       sync.RWMutex
	versions map[string]map[string]*VersionInfo // model -> version -> info
	rules    map[string]*RouteRule              // model -> 规则副本
}

// NewRouter 创建空路由器。
func NewRouter() *Router {
	return &Router{
		versions: make(map[string]map[string]*VersionInfo),
		rules:    make(map[string]*RouteRule),
	}
}

// RegisterVersion 注册（或更新）一个模型版本。
func (r *Router) RegisterVersion(info VersionInfo) error {
	if info.Model == "" || info.Version == "" {
		return NewGatewayError(ReasonRouteConflict, "register version requires non-empty model and version")
	}
	if info.State == "" {
		info.State = VersionActive
	}
	if !info.State.servable() && info.State != VersionOffline {
		return NewGatewayError(ReasonRouteConflict, "unknown version state %q", info.State)
	}
	if info.Backend == nil {
		return NewGatewayError(ReasonRouteConflict, "version %s/%s requires a backend", info.Model, info.Version)
	}
	cp := info
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.versions[info.Model] == nil {
		r.versions[info.Model] = make(map[string]*VersionInfo)
	}
	r.versions[info.Model][info.Version] = &cp
	return nil
}

// SetVersionState 修改版本生命周期状态（滚动升级的切换开关）。
func (r *Router) SetVersionState(model, version string, state VersionState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	vs, ok := r.versions[model]
	if !ok {
		return NewGatewayError(ReasonUnknownModel, "model %q not registered", model)
	}
	info, ok := vs[version]
	if !ok {
		return NewGatewayError(ReasonUnknownVersion, "version %q of model %q not registered", version, model)
	}
	if !state.servable() && state != VersionOffline {
		return NewGatewayError(ReasonRouteConflict, "unknown version state %q", state)
	}
	info.State = state
	return nil
}

// SetRule 安装/替换某模型的灰度规则，并立即做一致性与冲突校验。
// 校验内容：引用版本已注册且未下线、无重复引用、权重之和为 100。
func (r *Router) SetRule(rule RouteRule) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.validateLocked(rule); err != nil {
		return err
	}
	r.rules[rule.Model] = cloneRule(rule)
	return nil
}

// validateLocked 在读锁下校验规则（调用方需持有 r.mu 的读/写锁）。
func (r *Router) validateLocked(rule RouteRule) error {
	vs, modelKnown := r.versions[rule.Model]
	if !modelKnown {
		return NewGatewayError(ReasonUnknownModel,
			"cannot set rule: model %q has no registered versions", rule.Model)
	}

	// stable 引用校验。
	if rule.Stable != "" {
		info, ok := vs[rule.Stable]
		if !ok {
			return NewGatewayError(ReasonUnknownVersion,
				"route rule for %q references unregistered stable version %q", rule.Model, rule.Stable)
		}
		if info.State == VersionOffline {
			return NewGatewayError(ReasonVersionOffline,
				"route rule for %q references offline stable version %q", rule.Model, rule.Stable)
		}
	}

	// canary 引用 + 重复引用校验。
	canarySum := 0
	seen := map[string]bool{}
	if rule.Stable != "" {
		seen[rule.Stable] = true
	}
	names := make([]string, 0, len(rule.Canaries))
	for v := range rule.Canaries {
		names = append(names, v)
	}
	sort.Strings(names)
	for _, v := range names {
		w := rule.Canaries[v]
		if w < 0 {
			return NewGatewayError(ReasonRouteConflict,
				"negative canary weight %d for %s/%s", w, rule.Model, v)
		}
		if seen[v] {
			return NewGatewayError(ReasonRouteConflict,
				"version %q appears more than once in route rule for %q (stable and canary must be disjoint)",
				v, rule.Model)
		}
		seen[v] = true
		info, ok := vs[v]
		if !ok {
			return NewGatewayError(ReasonUnknownVersion,
				"route rule for %q references unregistered canary version %q", rule.Model, v)
		}
		if info.State == VersionOffline {
			return NewGatewayError(ReasonVersionOffline,
				"route rule for %q references offline canary version %q", rule.Model, v)
		}
		canarySum += w
	}

	stableWeight := rule.StableWeight
	if stableWeight < 0 {
		stableWeight = 100 - canarySum
	}
	if rule.Stable == "" {
		stableWeight = 0 // 没有 stable 版本时其权重必须为 0
	}
	if rule.StableWeight >= 0 && rule.Stable == "" && rule.StableWeight != 0 {
		return NewGatewayError(ReasonRouteConflict,
			"route rule for %q sets stable weight %d but has no stable version", rule.Model, rule.StableWeight)
	}
	if stableWeight < 0 {
		return NewGatewayError(ReasonRouteConflict,
			"canary weights sum to %d > 100 for model %q", canarySum, rule.Model)
	}
	if stableWeight+canarySum != 100 {
		return NewGatewayError(ReasonRouteConflict,
			"weights for model %q sum to %d (stable=%d, canary=%d), require 100",
			rule.Model, stableWeight+canarySum, stableWeight, canarySum)
	}
	return nil
}

func cloneRule(rule RouteRule) *RouteRule {
	cp := rule
	if rule.Canaries != nil {
		cp.Canaries = make(map[string]int, len(rule.Canaries))
		for k, v := range rule.Canaries {
			cp.Canaries[k] = v
		}
	}
	return &cp
}

// Resolve 解析请求应命中的后端版本。
// 任何异常都返回带明确 Reason 的错误，绝不静默回落到默认版本：
//   - 显式版本：未注册模型/版本、版本下线分别报 unknown_model / unknown_version / version_offline；
//   - 灰度路由：规则缺失报 no_candidate，规则与现状冲突报 route_conflict。
func (r *Router) Resolve(ctx context.Context, req RouteRequest) (*RouteDecision, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	vs, modelKnown := r.versions[req.Model]
	if !modelKnown {
		return nil, NewGatewayError(ReasonUnknownModel, "model %q is not registered", req.Model)
	}

	// 显式版本指定：只看注册表与生命周期状态，不受灰度权重影响。
	if req.Version != "" {
		info, ok := vs[req.Version]
		if !ok {
			return nil, NewGatewayError(ReasonUnknownVersion,
				"version %q of model %q is not registered", req.Version, req.Model)
		}
		if info.State == VersionOffline {
			return nil, NewGatewayError(ReasonVersionOffline,
				"version %q of model %q is offline", req.Version, req.Model)
		}
		return &RouteDecision{
			Model:   req.Model,
			Version: req.Version,
			Backend: info.Backend,
			Pinned:  true,
			Canary:  info.State == VersionCanary,
		}, nil
	}

	// 灰度路由。
	rule := r.rules[req.Model]
	if rule == nil {
		return nil, NewGatewayError(ReasonNoCandidate,
			"no route rule for model %q and request did not pin a version", req.Model)
	}
	// 规则安装后版本状态可能已变化（如某版本被下线），解析时重新校验。
	if err := r.validateLocked(*rule); err != nil {
		return nil, err
	}

	type weightSeg struct {
		version string
		canary  bool
		weight  int
	}
	stableWeight := rule.StableWeight
	if stableWeight < 0 {
		canarySum := 0
		for _, w := range rule.Canaries {
			canarySum += w
		}
		stableWeight = 100 - canarySum
	}
	segs := make([]weightSeg, 0, len(rule.Canaries)+1)
	if rule.Stable != "" && stableWeight > 0 {
		segs = append(segs, weightSeg{version: rule.Stable, weight: stableWeight})
	}
	names := make([]string, 0, len(rule.Canaries))
	for v := range rule.Canaries {
		names = append(names, v)
	}
	sort.Strings(names)
	for _, v := range names {
		if rule.Canaries[v] > 0 {
			segs = append(segs, weightSeg{version: v, canary: true, weight: rule.Canaries[v]})
		}
	}
	if len(segs) == 0 {
		return nil, NewGatewayError(ReasonNoCandidate,
			"route rule for model %q directs traffic to no servable version", req.Model)
	}

	bucket := hashBucket(req.ID, 100)
	cursor := 0
	var chosen weightSeg
	for _, s := range segs {
		cursor += s.weight
		if bucket < cursor {
			chosen = s
			break
		}
	}
	if chosen.version == "" { // 理论上不可达（权重和为 100），防御性处理
		return nil, NewGatewayError(ReasonRouteConflict,
			"weight selection failed for model %q (bucket %d)", req.Model, bucket)
	}

	info := vs[chosen.version]
	return &RouteDecision{
		Model:   req.Model,
		Version: chosen.version,
		Backend: info.Backend,
		Pinned:  false,
		Canary:  chosen.canary,
	}, nil
}

// hashBucket 对请求标识做 FNV-1a 哈希，返回 [0, n) 内的稳定落点。
// 同一请求标识在多次请求间总是命中同一版本，保证灰度行为可复现。
func hashBucket(id string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n))
}

// RuleSnapshot 返回某模型当前规则的副本（供测试与观测）。
func (r *Router) RuleSnapshot(model string) (RouteRule, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rule, ok := r.rules[model]
	if !ok {
		return RouteRule{}, false
	}
	return *cloneRule(*rule), true
}

// VersionSnapshot 返回某模型版本的状态（供测试与观测）。
func (r *Router) VersionSnapshot(model, version string) (VersionState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs, ok := r.versions[model]
	if !ok {
		return "", false
	}
	info, ok := vs[version]
	if !ok {
		return "", false
	}
	return info.State, true
}
