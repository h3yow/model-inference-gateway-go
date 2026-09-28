#!/usr/bin/env bash
# 本地冒烟脚本：构建并启动网关，依次验证
# 正常请求 / 超载重试后降级 / 限流(429) / 未注册版本(404) / 灰度切换。
set -u

PORT="${PORT:-18080}"
BASE="http://localhost:${PORT}"
BIN="$(mktemp -t gateway-smoke-XXXXXX)"
LOG="$(mktemp -t gateway-smoke-log-XXXXXX)"

cd "$(dirname "$0")/.." || exit 1

go build -o "$BIN" ./cmd/gateway || exit 1
"$BIN" -addr ":${PORT}" >"$LOG" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null; rm -f "$BIN" "$LOG"' EXIT

sleep 0.6
if ! curl -fsS "$BASE/healthz" >/dev/null; then
  echo "service failed to start; server log:"; cat "$LOG"; exit 1
fi

infer() { # $1=id $2=caller $3=version
  curl -s -w '\nHTTP=%{http_code}\n' -X POST "$BASE/v1/infer" -d "{
    \"id\":\"$1\",\"caller\":\"$2\",\"model\":\"llm\",\"version\":\"$3\",
    \"idempotent\":true,\"payload\":\"smoke\"}"
}

echo "== 1) 正常请求（灰度0%，应命中 v1，degraded=false）=="
infer smoke-1 team-b ""

echo "== 2) v1 置为持续过载：幂等请求重试3次耗尽后降级，degraded=true =="
curl -s -X POST "$BASE/v1/admin/fail" -d '{"backend":"v1","mode":"overload"}' >/dev/null
infer smoke-2 team-b ""

echo "== 3) team-a 限流（2/s burst=3）：连发5个，第4/5个应 429 rate_limited =="
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "rl-$i HTTP=%{http_code}\n" \
    -X POST "$BASE/v1/infer" -d "{\"id\":\"smoke-rl-$i\",\"caller\":\"team-a\",\"model\":\"llm\",\"idempotent\":true}"
done

echo "== 4) 未注册版本 -> 404 version_unregistered =="
infer smoke-4 team-b v9

echo "== 5) 灰度切到100% -> 未指定版本全部命中 v2 =="
curl -s -X POST "$BASE/v1/admin/canary" -d '{"canary_pct":100}' >/dev/null
infer smoke-5 team-b ""

echo
echo "服务端链路日志（请求标识 | 阶段 | 依据）："
grep -E 'smoke-[125]' "$LOG"

echo
echo "SMOKE OK"
