/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.6 11:28
***************************************************************/

package storer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type node struct {
	k        string
	v        Value
	expireAt int64 // 过期时间戳，expireAt = 0 表示已删除
}

// 内部缓存核心实现，包含双向链表和节点存储
type cache struct {
	// dlnk[0]是哨兵节点，记录链表头尾，dlnk[0][p]存储尾部索引，dlnk[0][n]存储头部索引
	dlnk [][2]uint16       // 双向链表，0 表示前驱，1 表示后继
	m    []node            // 预分配内存存储节点
	hmap map[string]uint16 // 键到节点索引的映射
	last uint16            // 最后一个节点元素的索引
}

type lru2Store struct {
	locks       []sync.Mutex                  // 每个桶的独立锁
	caches      [][2]*cache                   // 每个桶包含两级缓存
	onEvicted   func(key string, value Value) // 驱逐回调函数
	cleanupTick *time.Ticker                  // 定期清理定时器
	mask        int32                         // 用于哈希取模的掩码
	done        chan struct{}
	closed      int32 // 给 lru2Store 加一个 done 通道 + 关闭标记（防重复 Close）
}

const (
	expireDeleted  int64 = 0  // 已删除 / 无效
	expireNoExpire int64 = -1 // 永不过期（有效）
)

func isExpired(expireAt, now int64) bool {
	// expireAt > 0 才代表“有具体过期时间”
	return expireAt > 0 && now >= expireAt
}

func computeExpireAt(expiration time.Duration) int64 {
	// expiration <= 0 表示永不过期
	if expiration <= 0 {
		return expireNoExpire
	}
	return Now() + expiration.Nanoseconds()
}

// cap uint16：缓存能容纳的节点数量上限，最多有 cap 个元素。
// 因为是 uint16，理论最大是 65535。
// 一次性把所有数据结构的骨架搭好，后面只在这块预分配的内存里玩，不再频繁 new 节点/链表元素，从而降低 GC 压力、提升性能。
func Create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1),
		m:    make([]node, cap),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

// 把桶数量“向上对齐到 2 的幂”，并用对应的 mask 做位运算
// 分配 mask 的目的，就是为了后面能用 hash(key) & mask 快速把 key 映射到某个 bucket（分片）上，代替较慢的 %
// 16 -> 15, 17 -> 31
// maskOfNextPowOf2 计算大于或等于输入值的最近 2 的幂次方减一作为掩码值
func maskOfNextPowOf2(cap uint16) uint16 {
	// 先判断 cap 是不是 2 的幂
	if cap > 0 && cap&(cap-1) == 0 {
		// 已经是 2 的幂
		return cap - 1
	}

	// 把最高位下面全变成 1
	// 通过多次右移和按位或操作，将二进制中最高的 1 位右边的所有位都填充为 1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8)
}

func newLRU2Cache(opts Options) *lru2Store {
	if opts.BucketCount == 0 {
		opts.BucketCount = 16
	}
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}

	mask := maskOfNextPowOf2(opts.BucketCount)
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		mask:        int32(mask),
		done:        make(chan struct{}),
	}

	for i := range s.caches {
		s.caches[i][0] = Create(opts.CapPerBucket)
		s.caches[i][1] = Create(opts.Level2Cap)
	}

	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// 实现了 BKDR 哈希算法，用于计算键的哈希值
// hash 是具名返回值（named return value）相当于 var hash int32 = 0
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// 内部时钟，减少 time.Now() 调用造成的 GC 压力
// 如果每次都直接 time.Now()，高 QPS 下调用次数很夸张；
// 对 GC 来说，一堆短命对象 + syscall，也是一种压力。
// 等价于 var clock, p, n = time.Now().UnixNano(), uint16(0), uint16(1)
var (
	clock int64  = time.Now().UnixNano()
	p     uint16 = 0
	n     uint16 = 1
)

// 返回 clock 变量的当前值。atomic.LoadInt64 是原子操作，用于保证在多线程/协程环境中安全地读取 clock 变量的值
func Now() int64 {
	return atomic.LoadInt64(&clock)
}

// 从缓存中删除键对应的项
// 这个 del 并没有“物理删除”掉节点，而是做了“逻辑删除 + 移到空闲链表尾部”。
func (c *cache) del(key string) (*node, int, int64) {
	if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt != expireDeleted {
		e := c.m[idx-1].expireAt
		c.m[idx-1].expireAt = expireDeleted // 标记为已删除
		// n     uint16 = 1   p     uint16 = 0
		c.adjust(idx, n, p) // 移动到链表尾部
		return &c.m[idx-1], 1, e
	}

	return nil, 0, 0
}

// 把某个 key 从当前分片的两级缓存里都干掉，并按需触发回调
func (s *lru2Store) delete(key string, idx int32) bool {
	n1, s1, _ := s.caches[idx][0].del(key)
	n2, s2, _ := s.caches[idx][1].del(key)
	deleted := s1 > 0 || s2 > 0

	if deleted && s.onEvicted != nil {
		// 同一个 key 不会 同时 在 L1 和 L2 中有效存在；
		if n1 != nil && n1.v != nil {
			s.onEvicted(key, n1.v)
		} else if n2 != nil && n2.v != nil {
			s.onEvicted(key, n2.v)
		}
	}

	if deleted {
		//s.expirations.Delete(key)
	}

	// true：这次确实删掉了某一层的缓存条目；
	// false：找不到这个 key，或者已经被标记为删除过了
	return deleted
}

func (s *lru2Store) Get(key string) (Value, bool) {
	// 等价于 hash % (mask+1)，结果范围是 [0, mask]
	idx := hashBKRD(key) & s.mask

	// 对这个分片加锁
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	now := Now()

	// 首先检查一级缓存
	n1, status1, expireAt := s.caches[idx][0].del(key)
	if status1 > 0 {
		// 从一级缓存找到项目
		if isExpired(expireAt, now) {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		// 项目有效，迁移到二级缓存 L2
		s.caches[idx][1].put(key, n1.v, expireAt, s.onEvicted)
		fmt.Println("项目有效，将其移至二级缓存")
		return n1.v, true
	}

	// L1 没命中：去二级缓存 L2 查
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		if isExpired(n2.expireAt, now) {
			// 项目已过期，删除它
			s.delete(key, idx)
			fmt.Println("找到项目已过期，删除它")
			return nil, false
		}

		return n2.v, true
	}

	// 两级都没命中 → 返回失败
	return nil, false
}

func (s *lru2Store) Set(key string, value Value) error {
	// 9999999999999999 ns = 10,000,000,000 s ≈ 115.7 天
	// 这个 Set 默认就是“过期时间 ≈ 116 天以后”
	// return s.SetWithExpiration(key, value, 9999999999999999)
	return s.SetWithExpiration(key, value, 0) // 0 表示永不过期
}

// time.Duration 底层是 int64 的纳秒数
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// 计算过期时间 - 确保单位一致
	// expireAt := int64(0)
	// if expiration > 0 {
	// 	// now() 返回纳秒时间戳，确保 expiration 也是纳秒单位
	// 	expireAt = Now() + int64(expiration.Nanoseconds())
	// }

	expireAt := computeExpireAt(expiration)

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	// 放入一级缓存
	s.caches[idx][0].put(key, value, expireAt, s.onEvicted)

	return nil
}

// Delete 实现Storer接口
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()

	return s.delete(key, idx)
}

// dlnk[0][0] 存储尾结点索引, dlnk[0][1] 存储头结点索引
// 也就是按 LRU 顺序从“最久未使用”到“最新使用”遍历整条链表
// 遍历缓存中的所有有效项
func (c *cache) walk(now int64, walker func(key string, value Value, expireAt int64) bool) {
	// n = 1
	for idx := c.dlnk[0][n]; idx != 0; idx = c.dlnk[idx][n] {

		exp := c.m[idx-1].expireAt

		// 已删除
		if exp == expireDeleted {
			continue
		}

		// 已过期
		if isExpired(exp, now) {
			continue
		}
		// 同步回调walker
		// 有效（包括 exp=-1 永不过期）
		if !walker(c.m[idx-1].k, c.m[idx-1].v, exp) {
			return
		}
	}
}

// Clear 实现Storer接口
func (s *lru2Store) Clear() {
	now := Now()
	var keys []string

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(now, func(key string, value Value, expireAt int64) bool {
			keys = append(keys, key)
			return true
		})
		s.caches[i][1].walk(now, func(key string, value Value, expireAt int64) bool {
			// 检查键是否已经收集（避免重复）
			for _, k := range keys {
				if key == k {
					return true
				}
			}
			keys = append(keys, key)
			return true
		})

		s.locks[i].Unlock()
	}

	for _, key := range keys {
		s.Delete(key)
	}

	//s.expirations = sync.Map{}
}

// Len 实现Storer接口
func (s *lru2Store) Len() int {
	count := 0
	now := Now()

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(now, func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})
		s.caches[i][1].walk(now, func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

// Close 关闭缓存相关资源
// func (s *lru2Store) Close() {
// 	if s.cleanupTick != nil {
// 		// time.Ticker.Stop() 不会关闭 C 这个 channel，官方就是这么设计的：
// 		// 「Stop 会停止计时，但不会 close channel，避免误读成功。」
// 		s.cleanupTick.Stop()
// 	}
// }

func (s *lru2Store) Close() {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return
	}
	if s.cleanupTick != nil {
		s.cleanupTick.Stop()
	}
	// close(s.done) 必须只执行一次，否则会 panic
	close(s.done)
}

// func (s *lru2Store) cleanupLoop() {
// 	// 这是一个无限循环，每当 Ticker 产生一个 tick（往 C 写入时间）时，循环体执行一次。
// 	// 是一种 从 channel 持续读数据直到 channel 关闭 的写法，这里是每来一个 ticker 的 tick，就执行一次循环体，只是我们不关心具体 tick 时间，所以省略了接收变量
// 	for range s.cleanupTick.C {
// 		currentTime := Now()

// 		for i := range s.caches {
// 			s.locks[i].Lock()

// 			// 检查并清理过期项目
// 			var expiredKeys []string

// 			s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
// 				if expireAt > 0 && currentTime >= expireAt {
// 					expiredKeys = append(expiredKeys, key)
// 				}
// 				return true
// 			})

// 			s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
// 				if expireAt > 0 && currentTime >= expireAt {
// 					for _, k := range expiredKeys {
// 						if key == k {
// 							// 避免重复
// 							return true
// 						}
// 					}
// 					expiredKeys = append(expiredKeys, key)
// 				}
// 				return true
// 			})

// 			for _, key := range expiredKeys {
// 				s.delete(key, int32(i))
// 			}

// 			s.locks[i].Unlock()
// 		}
// 	}
// }

func (s *lru2Store) cleanupLoop() {
	for {
		select {
		case <-s.cleanupTick.C:
			now := Now()
			for i := range s.caches {
				s.locks[i].Lock()

				var expiredKeys []string

				s.caches[i][0].walk(now, func(key string, value Value, expAt int64) bool {
					if isExpired(expAt, now) {
						expiredKeys = append(expiredKeys, key)
					}
					return true
				})
				s.caches[i][1].walk(now, func(key string, value Value, expAt int64) bool {
					if isExpired(expAt, now) {
						expiredKeys = append(expiredKeys, key)
					}
					return true
				})

				for _, key := range expiredKeys {
					s.delete(key, int32(i))
				}

				s.locks[i].Unlock()
			}

		case <-s.done:
			return
		}
	}
}

// TODO:后续这个无限循环可能需要优化
func init() {
	go func() {
		// 无限循环
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// 调整节点在链表中的位置
// dlnk[0][0] 存储尾结点索引, dlnk[0][1] 存储头结点索引
// 当 f=0, t=1 时，移动到链表头部；否则f=1, t=0移动到链表尾部
func (c *cache) adjust(idx, f, t uint16) {
	// f=1,t=0即del操作，c.dlnk[idx][1] != 0说明其不是尾结点
	// f=0,t=1即更新操作, c.dlnk[idx][0] != 0说明其不是头结点
	if c.dlnk[idx][f] != 0 {
		// f=1,t=0即del操作: 把前驱的 next 指向后继   dlnk[idx][0]的next是dlnk[idx][1](装的dlnk[idx]的next结点)
		// f=0,t=1即更新操作: next 的 prev 指向 prev
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]

		// f=1,t=0即del操作:后继的 prev 指向前驱
		// c.dlnk[idx][1]：还是 next
		// c.dlnk[idx][0]：还是 prev
		// c.dlnk[next][0] = prev → next.prev = prev
		// f=0,t=1即更新操作:prev 的 next 指向 next
		// 原来的 prev <-> idx <-> next 变成了 prev <-> next
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]

		// f=1,t=0即del操作:清掉 idx 的 f 方向链接
		// f=0,t=1即更新操作:清掉 idx 的 prev
		c.dlnk[idx][f] = 0

		// f=1,t=0即del操作:让 idx 的 prev 指向原来的尾节点
		// f=0,t=1即更新操作:idx 的 next 指向当前 head
		c.dlnk[idx][t] = c.dlnk[0][t]

		// f=1,t=0即del操作:链表尾部就变成了：oldTail <-> idx
		// f=0,t=1即更新操作:旧 head 的 prev 指向 idx
		c.dlnk[c.dlnk[0][t]][f] = idx

		// f=1,t=0即del操作:把哨兵里的 tail 指针更新到 idx，也就是“现在 idx 成了新的尾节点”。
		// f=0,t=1即更新操作:更新 head 指针
		c.dlnk[0][t] = idx
	}
}

// 向缓存中添加项，如果是新增返回 1，更新返回 0
func (c *cache) put(key string, val Value, expireAt int64, onEvicted func(string, Value)) int {
	// key 存在
	if idx, ok := c.hmap[key]; ok {
		c.m[idx-1].v, c.m[idx-1].expireAt = val, expireAt
		// head 代表最近使用（MRU）
		// tail 代表最久未使用（LRU）
		// 所以访问/更新后要移到 head
		c.adjust(idx, p, n) // 刷新到链表头部
		return 0
	}

	// key 不存在 且 cache 已满
	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][p]-1]
		if onEvicted != nil && (*tail).expireAt > 0 {
			onEvicted((*tail).k, (*tail).v)
		}

		// 从 hmap 删掉旧 key 的映射
		delete(c.hmap, (*tail).k)

		// 不分配新槽位；直接把 LRU 尾部那个节点改成新数据; map 映射更新到新 key
		c.hmap[key], (*tail).k, (*tail).v, (*tail).expireAt = c.dlnk[0][p], key, val, expireAt

		// 把这个“复用后的节点”移到 head（最新）
		c.adjust(c.dlnk[0][p], p, n)

		return 1
	}

	// key 不存在 且没满 → 分配新槽位插到 head
	// dlnk索引从 1 开始用，m 下标从 0，用 idx-1 对齐
	c.last++

	// 表示现在链表是空的（第一次插入）
	if len(c.hmap) <= 0 {
		// 先把 tail 指向新节点
		// dlnk[0][0]尾结点，dlnk[0][1]头结点
		c.dlnk[0][p] = c.last
	} else {
		// c.dlnk[0][n] 是当前 head;c.dlnk[head][p] = last 让旧 head 的 prev 指向新节点
		c.dlnk[c.dlnk[0][n]][p] = c.last
	}

	// 初始化新节点并更新链表指针
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt
	// 新节点的 prev = 0（暂时没有前驱，因为它将成为 head）;新节点的 next = 旧 head
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][n]}
	c.hmap[key] = c.last
	c.dlnk[0][n] = c.last

	return 1
}

// 从缓存中获取键对应的节点和状态
func (c *cache) get(key string) (*node, int) {
	if idx, ok := c.hmap[key]; ok {
		c.adjust(idx, p, n)
		// 返回节点 + 命中标识
		return &c.m[idx-1], 1
	}
	return nil, 0
}

// 在指定分片、指定层级(L1/L2)里查 key，并额外做一次过期/有效性检查
func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		now := Now()

		// 已删除
		if n.expireAt == expireDeleted {
			return nil, 0
		}

		// 有过期时间且已过期
		if isExpired(n.expireAt, now) {
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}
