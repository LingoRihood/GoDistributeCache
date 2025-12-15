package gocache

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/LingoRihood/GoDistributeCache/storer"
)

func newTestCache() *Cache {
	opts := DefaultCacheOptions()
	opts.CacheType = storer.LRU2
	// 为了让测试更快一点（尤其是过期相关），把清理间隔调小
	opts.CleanupTime = 50 * time.Millisecond
	return NewCache(opts)
}

func TestCache_AddGet_Stats(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	// 未初始化时 Get：直接 miss（Cache.Get 里会 misses++）
	// ok 一定是 false v 返回的是 ByteView{}，所以 v.Len()==0
	if v, ok := c.Get(ctx, "no_such_key"); ok || v.Len() != 0 {
		t.Fatalf("expected miss before init, got ok=%v, v=%q", ok, v.String())
	}

	// Add 会触发 ensureInitialized
	c.Add("k1", ByteView{b: []byte("v1")})

	v, ok := c.Get(ctx, "k1")
	if !ok {
		t.Fatalf("expected hit, got miss")
	}
	if got := v.String(); got != "v1" {
		t.Fatalf("expected v1, got %q", got)
	}

	// 再来一次 miss
	_, _ = c.Get(ctx, "no_such_key2")

	stats := c.Stats()
	if stats["initialized"].(bool) != true {
		t.Fatalf("expected initialized=true, got %+v", stats)
	}
	if stats["closed"].(bool) != false {
		t.Fatalf("expected closed=false, got %+v", stats)
	}

	hits := stats["hits"].(int64)
	misses := stats["misses"].(int64)

	// hits 至少 1（k1 命中一次）
	if hits < 1 {
		t.Fatalf("expected hits>=1, got %d", hits)
	}
	// misses 至少 2（no_such_key、no_such_key2）
	if misses < 2 {
		t.Fatalf("expected misses>=2, got %d", misses)
	}

	// hit_rate 应该在 [0,1]
	if hr, ok := stats["hit_rate"].(float64); ok {
		if hr < 0 || hr > 1 {
			t.Fatalf("invalid hit_rate=%v", hr)
		}
	}
}

func TestCache_AddWithExpiration(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	// 100ms 后过期
	expAt := time.Now().Add(100 * time.Millisecond)
	c.AddWithExpiration("kexp", ByteView{b: []byte("vexp")}, expAt)

	// 立刻 Get 应该命中
	v, ok := c.Get(ctx, "kexp")
	if !ok {
		t.Fatalf("expected hit before expiration")
	}
	if v.String() != "vexp" {
		t.Fatalf("expected vexp, got %q", v.String())
	}

	// 等到过期以后再 Get
	// 注意：你的 lru2.go 用了内部 clock（每 100ms 校准），所以这里多等一点更稳
	time.Sleep(350 * time.Millisecond)

	_, ok = c.Get(ctx, "kexp")
	if ok {
		t.Fatalf("expected miss after expiration, got hit")
	}
}

func TestCache_Delete(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	c.Add("kdel", ByteView{b: []byte("vdel")})

	if ok := c.Delete("kdel"); !ok {
		t.Fatalf("expected Delete(kdel)=true")
	}

	_, ok := c.Get(ctx, "kdel")
	if ok {
		t.Fatalf("expected miss after delete")
	}

	// 删除不存在的 key
	if ok := c.Delete("no_such_key"); ok {
		t.Fatalf("expected Delete(no_such_key)=false")
	}
}

func TestCache_Clear_ResetsStats(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	c.Add("a", ByteView{b: []byte("1")})
	c.Add("b", ByteView{b: []byte("2")})

	// 触发一次 hit 和一次 miss
	_, _ = c.Get(ctx, "a")
	_, _ = c.Get(ctx, "no_such_key")

	if c.Len() != 2 {
		t.Fatalf("expected Len=2 before Clear, got %d", c.Len())
	}

	c.Clear()

	if got := c.Len(); got != 0 {
		t.Fatalf("expected Len=0 after Clear, got %d", got)
	}

	stats := c.Stats()
	if stats["hits"].(int64) != 0 {
		t.Fatalf("expected hits reset to 0, got %v", stats["hits"])
	}
	if stats["misses"].(int64) != 0 {
		t.Fatalf("expected misses reset to 0, got %v", stats["misses"])
	}
}

func TestCache_Close(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	c.Add("k", ByteView{b: []byte("v")})
	if _, ok := c.Get(ctx, "k"); !ok {
		t.Fatalf("expected hit before close")
	}

	c.Close()

	// close 后 Get 一律 miss
	if _, ok := c.Get(ctx, "k"); ok {
		t.Fatalf("expected miss after close")
	}

	// close 后 Add 不应 panic（内部会直接 return）
	c.Add("k2", ByteView{b: []byte("v2")})
	if _, ok := c.Get(ctx, "k2"); ok {
		t.Fatalf("expected miss for added-after-close key")
	}

	stats := c.Stats()
	if stats["closed"].(bool) != true {
		t.Fatalf("expected closed=true, got %+v", stats)
	}
	// Close 会把 initialized 置 0
	if stats["initialized"].(bool) != false {
		t.Fatalf("expected initialized=false after Close, got %+v", stats)
	}
}

func TestCache_Concurrent_AddGet(t *testing.T) {
	c := newTestCache()
	ctx := context.Background()

	const goroutines = 50

	// 每个 goroutine 做 50 次循环
	const perG = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range perG {
				key := makeKey(id, i)
				c.Add(key, ByteView{b: []byte("v")})
				_, _ = c.Get(ctx, key)
			}
		}(g)
	}

	wg.Wait()

	// 粗略检查：至少有一些元素，且不会 panic
	if c.Len() == 0 {
		t.Fatalf("expected Len>0 after concurrent add/get")
	}
}

// makeKey 生成测试 key（不引入 fmt，避免额外开销）
func makeKey(g, i int) string {
	// 简单拼接：g-i
	// g 和 i 都比较小，这个够用
	return "g" + itoa(g) + "-" + itoa(i)
}

// 把整数转换成字符串
// 用这个函数避免 fmt.Sprintf("%d", x) 带来的额外开销（fmt 重、分配多），尤其在并发测试里更轻量
func itoa(x int) string {
	if x == 0 {
		return "0"
	}
	neg := false
	if x < 0 {
		neg = true
		x = -x
	}
	var buf [32]byte
	n := 0
	for x > 0 {
		d := x % 10
		buf[n] = byte('0' + d)
		n++
		x /= 10
	}
	// reverse
	for i := 0; i < n/2; i++ {
		buf[i], buf[n-1-i] = buf[n-1-i], buf[i]
	}
	if neg {
		return "-" + string(buf[:n])
	}
	return string(buf[:n])
}
