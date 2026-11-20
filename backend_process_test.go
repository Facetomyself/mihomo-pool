//go:build !embedded

package mihomopool

import (
	"errors"
	"strings"
	"testing"
)

var errBoom = errors.New("boom")

func TestDescribeExit(t *testing.T) {
	if got := describeExit(errBoom); !strings.Contains(got, "boom") {
		t.Fatalf("describeExit 不对: %s", got)
	}
}

func TestProcessBackendName(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	b, err := newBackend(o)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	if b.Name() != "process" {
		t.Fatalf("后端名应为 process，得到 %q", b.Name())
	}
	if b.Running() {
		t.Fatal("新建后端不应处于运行态")
	}
	if b.PID() != 0 {
		t.Fatalf("未运行时 PID 应为 0，得到 %d", b.PID())
	}
}

func TestProcessBackendResolveBinaryMissing(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.Binary = "/nonexistent/mihomo-xyz"
	_ = o.normalize()
	b, _ := newBackend(o)
	pb := b.(*processBackend)
	if _, err := pb.resolveBinary(); err == nil {
		t.Fatal("指向不存在的 Binary 应报错")
	}
}
