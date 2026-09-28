# 模型推理服务网关（Go）

一个本地可运行的模型推理网关：统一接收上层业务对多个模型/版本的推理请求，在本地完成
**按调用方限流与并发控制 → 模型版本路由与灰度 → 带退避的重试 → 失败降级**，并把请求分发到模拟的推理后端。

所有失败都返回互斥、机器可读的 `reason`，调用方可以明确区分"被限流 / 后端过载 / 版本问题 / 降级结果"。

## 目录结构

```
internal/gateway/
  errors.go        失败原因（RejectReason）与网关错误类型
  quota.go         按调用方的令牌桶限流 + 并发槽
  router.go        版本注册表、生命周期、灰度规则校验、确定性权重路由
  backend.go       后端抽象、请求/响应、后端失败分类
  retry.go         重试策略：可重试判定 + 指数退避（抖动/上限）
  degrade.go       降级结果构造（Degraded 标记 + 原因说明）
  gateway.go       编排入口与事件日志
  mock_backend.go  本地模拟后端：脚本化/持续注入 超载、超时、永久失败
cmd/gateway/       HTTP 服务 main
```

## 处理链路

```
请求 → ① 配额（并发槽 + 令牌桶，同一临界区判定）
     → ② 路由（显式 pin 版本，或按请求 ID 哈希做灰度权重落点）
     → ③ 后端调用（失败分类；幂等且可重试才按指数退避重试，有上限）
     → ④ 仍失败则降级（响应带 degraded=true 与 fallback_note）
```

## 1. 限流与并发规则

配额按调用方（`caller`）配置，两部分**在每调用方独立的同一把锁内原子判定**：

| 控制项 | 字段 | 语义 |
| --- | --- | --- |
| QPS 限流 | `RateLimitPerSec` / `Burst` | 令牌桶：容量 `Burst`，每秒补充 `RateLimitPerSec` 个令牌；桶空即拒绝 |
| 并发上限 | `MaxInflight` | 当前在途请求达到上限即拒绝；请求结束归还槽位 |

- 判定顺序：**先查并发槽，再取令牌**。并发槽满时不消耗令牌。
- 并发安全：取令牌与占槽在同一临界区完成，高并发竞争下也**不会超额放行**
  （`TestQuotaConcurrentInflightNoOverAdmit` 用 200 个 goroutine 竞争 5 个槽，断言放行数恰好为 5、峰值在途不超过 5）。
- 限流状态（令牌余额、在途数）在多次请求之间保持。
- 未配置的调用方回退到默认配额键 `"*"`；连默认配额都没有则拒绝。

两种配额拒绝原因可区分：

- `rate_limited`（令牌桶空，HTTP 429）
- `too_many_inflight`（并发槽满，HTTP 429）

## 2. 模型版本路由与灰度

### 版本生命周期

`active`（稳定承接流量）、`canary`（灰度中）、`offline`（已下线）。
滚动升级通过 `SetVersionState` 与 `SetRule` 在线切换，无需重启。

### 路由方式

1. **显式版本（pin）**：请求带 `version` 时只查注册表与生命周期状态，
   不受灰度权重影响。
2. **灰度路由**：不带 `version` 时按规则权重分流。
   权重落点 = `FNV-1a(request_id) % 100`，**同一请求标识稳定命中同一版本**，行为可复现。

### 规则校验（安装时 + 每次解析时）

`stable 权重 + Σ canary 权重 = 100`（`StableWeight<0` 表示自动取 `100-Σcanary`）。
以下情况**显式失败，绝不静默回落到默认版本**：

| 场景 | reason |
| --- | --- |
| 模型未注册 | `unknown_model` |
| 显式版本未注册 | `unknown_version` |
| 显式版本已下线 | `version_offline` |
| 权重和不为 100 / 负权重 / canary 超 100 | `route_conflict` |
| 规则引用了未注册或已下线版本 | `unknown_version` / `version_offline` |
| 同一版本同时出现在 stable 与 canary | `route_conflict` |
| 未 pin 版本且模型没有灰度规则 | `no_candidate` |
| 规则安装后其中版本被下线（滚动升级中途） | 解析时重新校验，返回 `version_offline` / `route_conflict` |

灰度比例示例（`TestRouteCanaryWeightDistribution`、`TestRouteRollingCanaryShift`）：
80/20 规则下 1000 个 ID 实测 v2 占比 ≈ 0.20；在线切到 50/50 后 ≈ 0.50。

## 3. 异常、重试与降级语义

### 后端失败分类

| 分类 | 含义 | 可重试 |
| --- | --- | --- |
| `overload` | 后端过载（429/503 语义） | 是 |
| `timeout` | 后端超时 | 是 |
| `transient` | 瞬时错误（断连等） | 是 |
| `permanent` | 永久失败（参数错误等） | 否 |

### 重试条件（三者必须同时满足）

1. 失败属于可重试分类；
2. **请求声明幂等**（`idempotent=true`）——非幂等请求一律不重试；
3. 尚未达到 `MaxAttempts`（含首次调用的总尝试上限）。

退避：`min(BaseDelay × Factor^(attempt-1), MaxDelay)`，再叠加 `[1-Jitter,1+Jitter]` 抖动；
退避等待可被整体超时/取消打断。

### 结果矩阵

| 情况 | 结果 | 调用方如何识别 |
| --- | --- | --- |
| 成功 | 正常响应 | `degraded=false` |
| 可重试失败，重试后成功 | 正常响应 | `degraded=false` |
| 可重试失败，但**非幂等** | 立即降级，不重试 | `degraded=true`，note 中 `reason=non_retryable` |
| 可重试失败，幂等，次数耗尽 | 降级 | `degraded=true`，note 中 `reason=retries_exhausted`，并带 `last=backend_overload/backend_timeout/...` 底层原因 |
| 退避期间整体超时 | 降级 | `degraded=true`，`reason=backend_timeout` |
| `permanent` 永久失败 | **硬失败，不重试、不降级** | HTTP 错误，`reason=backend_error` |
| 配额/路由类失败 | 硬失败（不触达后端） | 对应 `reason` |

降级响应的契约：`degraded` 恒为 `true`、`fallback_note` 非空并写明原因，
`output` 为空，避免调用方把合成结果误当作有效推理结论。

### 全部 reason 一览

`rate_limited`、`too_many_inflight`、`unknown_model`、`unknown_version`、
`version_offline`、`route_conflict`、`no_candidate`、`backend_timeout`、
`backend_overload`、`backend_error`、`non_retryable`、`retries_exhausted`。

HTTP 状态映射：限流/过载 → 429；未注册模型/版本 → 404；下线/规则冲突/无候选 → 409；
后端超时 → 504；其余后端错误 → 502。

## 4. 本地验证

### 运行测试

```bash
go test ./...                 # 全部用例
go test -race ./...           # 带竞态检测（并发限流用例）
go test -v ./... -run TestQuotaConcurrentInflightNoOverAdmit
go test -v ./... -run TestRouteCanary
go test -v ./... -run TestInfer
```

测试覆盖：

- **并发限流**：200 goroutine 竞争 5 并发槽，断言恰好放行 5、无超额；令牌桶突发与补充；
- **灰度切换**：80/20 → 50/50 在线切换、同 ID 落点稳定、各类规则冲突拒绝、下线版本不回落；
- **异常降级**：重试后恢复、非幂等不重试直接降级、重试耗尽降级、永久失败硬失败、
  过载/超时/永久失败原因可区分、部分失败（前 N 次失败后恢复）。
- 测试日志（`testing` 的 `-v` 输出）打印每个请求的 `request_id` 与完整判定链路
  （`stage=quota/route/attempt/retry/degrade/reject/success` 与 `reason`）。

### 运行 HTTP 服务

```bash
go run ./cmd/gateway
# 服务监听 :8080
```

预置模型与配额：

- `text-gen`：`v1`=stable 健康；`v2`=canary 20%，前两次调用注入过载后恢复（演示重试成功）；
- `flaky`：`v1` 始终超时（演示幂等请求重试耗尽后降级）；
- `boom`：`v1` 永久失败（演示硬失败）；
- 配额：`svc-a` 5/s、突发 5、并发 2；`svc-b` 2/s、突发 2、并发 1；`*` 默认 1/s。

```bash
# 正常灰度请求
curl -s -X POST localhost:8080/v1/infer -d \
  '{"request_id":"r1","caller":"svc-a","model":"text-gen","idempotent":true}'

# pin 灰度版本（前两次过载，退避重试后第 3 次成功）
curl -s -X POST localhost:8080/v1/infer -d \
  '{"request_id":"r2","caller":"svc-a","model":"text-gen","version":"v2","idempotent":true}'

# 持续超时 + 幂等 => 重试 3 次后降级
curl -s -X POST localhost:8080/v1/infer -d \
  '{"request_id":"r3","caller":"svc-a","model":"flaky","idempotent":true}'

# 持续超时 + 非幂等 => 不重试，直接降级
curl -s -X POST localhost:8080/v1/infer -d \
  '{"request_id":"r4","caller":"svc-a","model":"flaky","idempotent":false}'

# 触发限流（svc-b 突发容量 2）
for i in 1 2 3 4; do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/v1/infer -d \
    "{\"request_id\":\"rl$i\",\"caller\":\"svc-b\",\"model\":\"text-gen\"}"
done
```

服务端日志对每个请求打印 `request_id`、所处阶段、原因与判定依据，例如：

```
[r2] stage=attempt reason=backend_overload backend text-gen/v2 attempt 1/3 failed ... (idempotent=true, retryable-kind=true)
[r2] stage=retry   reason=none           retrying text-gen/v2 after 99ms (next attempt 2/3)
[r2] stage=success reason=none           backend text-gen/v2 succeeded on attempt 3
```
