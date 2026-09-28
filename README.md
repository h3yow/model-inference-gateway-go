# 模型推理服务网关（Go）

一个**本地可运行**的模型推理网关：统一接收上层业务对多个模型版本的推理请求，
在本地完成「按调用方限流 → 模型版本/灰度路由 → 有限重试 → 失败降级」，
并把请求分发给模拟的推理后端。无任何外部依赖（仅 Go 标准库）。

## 目录结构

```
cmd/gateway/             本地 HTTP 服务（含模拟后端与管理接口）
internal/types/          统一错误类型 GatewayError 与可区分原因 Reason
internal/ratelimit/      按调用方的令牌桶限流 + 并发上限（互斥原子判定）
internal/routing/        模型版本注册表、版本生命周期、灰度规则与确定性分流
internal/backend/        Backend 接口与可编程的模拟后端（ok/overload/timeout/partial）
internal/retry/          幂等校验 + 可重试分类 + 指数退避 + 次数上限
internal/gateway/        组装以上组件的统一入口，附带请求链路日志
```

## 快速开始

```bash
go run ./cmd/gateway                 # 默认监听 :8080
# 或指定端口
go run ./cmd/gateway -addr :18080
```

发起推理：

```bash
curl -s -X POST localhost:8080/v1/infer -d '{
  "id": "req-001", "caller": "team-a", "model": "llm",
  "version": "", "idempotent": true, "payload": "hello"
}'
```

响应中 `degraded=true` 即表示这是**降级结果**（来自降级后端，非主路径正常结果），
`degrade_note` 说明主路径失败原因与重试次数，`attempts` 是实际尝试次数。

管理接口（仅用于本地演示与制造故障/灰度场景）：

```bash
# 制造后端故障：mode = ok | overload | timeout | partial
curl -s -X POST localhost:8080/v1/admin/fail    -d '{"backend":"v1","mode":"overload"}'
# 滚动升级：调整 v2 灰度流量百分比 [0,100]
curl -s -X POST localhost:8080/v1/admin/canary  -d '{"canary_pct":20}'
```

## 一、限流与并发规则

每个调用方（`caller`）独立配置配额（见 `internal/ratelimit/ratelimit.go`）：

| 配额 | 含义 |
| --- | --- |
| `RatePerSecond` | 令牌桶填充速率（QPS） |
| `BurstCapacity` | 令牌桶容量（突发上限） |
| `MaxConcurrent` | 同时在途请求数上限 |

每个请求的判定顺序固定为：

1. **调用方是否已配置**：未配置 → `caller_not_configured`（不假设默认配额）；
2. **令牌桶速率**：令牌不足 → `rate_limited`（HTTP 429）；
3. **并发上限**：在途数已满 → `caller_concurrency`（HTTP 429）；
4. 通过后在**同一把调用方锁内**同时「扣令牌 + 占并发名额」；请求结束时
   `release()` 归还并发名额（令牌不退还，被拒请求不消耗任何资源）。

令牌桶按经过时间惰性填充（`tokens = min(capacity, tokens + elapsed*rate)`），
因此限流状态在多次请求之间保持一致。判定与占用处于同一临界区，
即使大量 goroutine 同时竞争，也**不会因竞争而放行超额请求**
（由 `TestConcurrencyLimitNoOverAdmit` 在 `-race` 下验证：50 个 goroutine
抢 3 个名额，恰好 3 个成功，峰值并发 ≤ 3）。

## 二、版本路由与灰度规则

见 `internal/routing/routing.go`。

### 显式版本

请求带 `version` 时严格匹配，**绝不静默回落**：

- 模型或版本未注册 → `version_unregistered`（HTTP 404）；
- 版本已下线（`SetOffline`）→ `version_offline`（HTTP 409）。

### 灰度（未指定版本，即请求"任意可用版本"）

为模型设置 `CanaryRule{Stable, Canary, CanaryPct}` 后：

- 以请求标识做稳定哈希（FNV-1a），`hash % 100 < CanaryPct` 走灰度版本，
  否则走稳定版本；同一请求标识的路由结果**确定性可复现**；
- `CanaryPct=0` 表示全量旧版本，`=100` 表示全量新版本，可直接用于滚动升级，
  调整比例即时生效、无需重启；
- 分流区间与判定依据（`hash` 值、命中区间）会打印到链路日志。

**灰度配置冲突一律拒绝写入**（`canary_conflict`，HTTP 409）：

- 百分比不在 `[0,100]`；
- stable 与 canary 是同一版本；
- 引用的模型/版本未注册，或任一版本已下线。

若一个模型存在多个在线版本、却没有灰度规则，网关**拒绝隐式选择默认版本**，
同样返回 `canary_conflict`——宁可不服务，也不静默把流量落到意料之外的版本。

## 三、异常、重试与降级语义

见 `internal/retry/retry.go` 与 `internal/gateway/gateway.go`。

模拟后端的故障模式与分类：

| 故障模式 | 含义 | 是否可重试 |
| --- | --- | --- |
| `ok` | 正常返回 | — |
| `overload` | 后端线程池打满，立即拒绝 | 是 |
| `timeout` | 处理超时 | 是 |
| `partial` | 返回损坏/部分结果（业务性失败） | **否** |

### 重试条件（必须同时满足）

1. 请求 `idempotent=true`：非幂等请求失败立即返回，**绝不重试**；
2. 错误属于可重试类别（当前为 `backend_overload` / `backend_timeout`）；
   `backend_failed`（含 partial）等业务错误立即返回；
3. 未达到尝试次数上限 `MaxAttempts`（含首次）。

退避为指数退避 `min(InitialBackoff * 2^(n-1), MaxBackoff)`。
重试耗尽返回 `retry_exhausted`（HTTP 429），错误消息中**保留最后一次的具体原因**
（例如 `last reason=backend_timeout`），调用方既能看到"重试耗尽"也能看到根因。

### 降级

主路径（含重试）失败后，若配置了降级后端，则再尝试一次降级后端：

- 降级成功：HTTP 200，但响应体显式标记 `"degraded": true`，
  并附 `degrade_note`（主路径错误 + 尝试次数），调用方可明确区分这不是正常结果；
- 降级后端也失败：返回 `fallback_failed`（HTTP 502），消息同时包含主/备两端错误；
- 未配置降级后端时，主路径错误原样透出。

注意：限流/并发/路由类失败发生在"调用后端"之前，**不会触发降级**——
降级只针对已经发生的后端执行失败。

### 全部可区分原因（响应体 `error.reason`）

`caller_not_configured`、`rate_limited`、`caller_concurrency`、
`version_unregistered`、`version_offline`、`canary_conflict`、
`backend_overload`、`backend_timeout`、`backend_failed`、
`retry_exhausted`、`fallback_failed`、`backend_not_configured`。

## 四、本地验证方式

### 自动化测试（覆盖三类核心场景）

```bash
go test ./... -race -v
```

- `internal/ratelimit`：并发竞争不超额（50 抢 3）、限流状态跨请求一致
  （注入时钟验证令牌补充）、限流与并发拒绝原因可区分；
- `internal/routing`：未注册/下线/冲突拒绝、灰度 20%→80% 比例精确、
  边界 `[0,pct)`、确定性、`0%/100%` 切换；
- `internal/retry`：超载恢复后重试成功、非幂等不重试、partial 不重试、
  耗尽保留末次原因、指数退避序列与上限；
- `internal/gateway`：端到端串联——超载重试后降级并标记 `degraded`、
  超时耗尽、partial 一次即降级、降级再失败、灰度 0%→100% 切换、
  入口并发拒绝、链路日志含请求标识与路由依据。

所有测试日志均打印请求标识与判定依据（中文标注"判定依据"）。

### 手工脚本

```bash
./scripts/smoke.sh        # 启动服务并自动跑一遍：正常/过载降级/限流/灰度
```

也可以按 README 中的 curl 步骤手工验证。服务端日志每行形如：

```
17:15:31.616 | request=req-001 | stage=routing   | 命中 model=llm version=v1 backend=llm-v1 依据=灰度规则 stable=v1 canary=v2 pct=0%, hash=43, 落在稳定版本区间
17:15:31.616 | request=req-001 | stage=execute   | 成功 backend=llm-v1 attempts=1
```
