package scan_server

import (
	"fmt"
	"testing"
	"time"
)

func TestUnixToStr(t *testing.T) {
	// 时间戳转时间
	nowUnix := time.Now().Unix()
	nowStr := unixToStr(nowUnix, "2006-01-02")
	fmt.Println(nowStr)
}
