package gateway

import (
	"strconv"
	"sync/atomic"
	"time"
)

// newIDGenerator 返回进程内唯一请求标识生成器。
func newIDGenerator() func() string {
	var seq uint64
	prefix := "req-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-"
	return func() string {
		return prefix + strconv.FormatUint(atomic.AddUint64(&seq, 1), 36)
	}
}
