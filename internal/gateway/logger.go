package gateway

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// StdLogger 是 Logger 的一个线程安全实现：
// 每行打印 时间 | 请求标识 | 阶段 | 判定依据。
type StdLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdLogger 创建输出到 stdout 的日志器。
func NewStdLogger() *StdLogger { return &StdLogger{w: os.Stdout} }

// NewLogger 创建输出到指定 writer 的日志器（测试可传 bytes.Buffer）。
func NewLogger(w io.Writer) *StdLogger { return &StdLogger{w: w} }

func (l *StdLogger) Logf(requestID, stage, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%s | request=%-12s | stage=%-9s | %s\n",
		time.Now().Format("15:04:05.000"), requestID, stage, fmt.Sprintf(format, args...))
}
