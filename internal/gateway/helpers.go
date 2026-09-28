package gateway

import (
	"strconv"
	"time"
)

// now 可在测试中通过包级变量替换（当前实现直接用真实时钟）。
var gatewayNow = time.Now

func (g *Gateway) now() time.Time { return gatewayNow() }

func itoa(n int) string { return strconv.Itoa(n) }
