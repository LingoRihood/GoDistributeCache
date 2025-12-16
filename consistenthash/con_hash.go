/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.9 11:49
***************************************************************/

package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 一致性哈希实现
type Map struct {
	// 只保护“哈希环结构”（keys/hashMap/nodeReplicas/nodeCounts指针map）
	mu           sync.RWMutex   // 读写锁，保证并发安全
	config       *Config        // 配置信息
	keys         []int          // 哈希环上的所有虚拟节点位置，按顺序排列(虚拟节点位置数组)
	hashMap      map[int]string // 从哈希值到实际节点名称的映射(虚拟节点 hash -> 真实节点)
	nodeReplicas map[string]int // 每个实际节点对应的虚拟节点数量(真实节点 -> 虚拟节点数)

	// nodeCounts 里存的是 “计数器指针”，Get 路径只做 atomic.AddInt64，不再写锁
	// map 的增删只在 mu 写锁下进行
	nodeCounts    map[string]*int64 // 记录每个节点处理的请求数(真实节点 -> *请求计数)
	totalRequests int64             // 记录总请求数，用于负载均衡计算

	// balancer goroutine control
	stopCh chan struct{}
	once   sync.Once
}

// Option 配置选项
type Option func(*Map)

// New 创建一致性哈希实例
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]*int64),
		stopCh:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	// 兜底：避免外部传 nil config
	if m.config == nil {
		m.config = DefaultConfig
	}

	if m.config.HashFunc == nil {
		m.config.HashFunc = DefaultConfig.HashFunc
	}

	// 后台 Rebalance 协程
	m.startBalancer() // 启动负载均衡器
	return m
}

// Close 停止后台负载均衡协程（避免 goroutine 泄漏）
func (m *Map) Close() {
	m.once.Do(func() {
		close(m.stopCh)
	})
}

// 将checkAndRebalance移到单独的goroutine中
// func (m *Map) startBalancer() {
// 	go func() {
// 		ticker := time.NewTicker(time.Second)
// 		defer ticker.Stop()

// 		for range ticker.C {
// 			m.checkAndRebalance()
// 		}
// 	}()
// }

// startBalancer 后台每秒检查一次是否需要 rebalance（可 Close 停止）
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.checkAndRebalance()
			case <-m.stopCh:
				return
			}
		}
	}()
}

// WithConfig 设置配置
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// addNode 添加节点的虚拟节点(给真实节点添加 replicas 个虚拟节点)
// func (m *Map) addNode(node string, replicas int) {
// 	for i := 0; i < replicas; i++ {
// 		// 生成每个虚拟节点的 hash 值
// 		// node = "10.0.0.1:6379"，i = 0 → "10.0.0.1:6379-0"
// 		// node = "10.0.0.1:6379"，i = 1 → "10.0.0.1:6379-1"
// 		// 把字符串转换成字节切片，传给哈希函数
// 		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
// 		m.keys = append(m.keys, hash)
// 		m.hashMap[hash] = node
// 	}
// 	m.nodeReplicas[node] = replicas
// }

// addNodeLocked 在写锁下调用：为真实节点添加 replicas 个虚拟节点
func (m *Map) addNodeLocked(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}

	m.nodeReplicas[node] = replicas

	// 初始化计数器（只在第一次添加时创建）
	if _, ok := m.nodeCounts[node]; !ok {
		// 分配一块 int64 内存, 初始值自动为 0, 把这块内存的地址（*int64）存进 map
		m.nodeCounts[node] = new(int64)
	}
}

// Add 添加节点
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}

		// 为节点添加虚拟节点
		// 如果已存在：你可以选择直接跳过或覆盖，这里选择“覆盖 replicas=DefaultReplicas”
		// 先移除旧的虚拟节点再加新的，避免重复
		if m.nodeReplicas[node] > 0 {
			m.removeLocked(node)
		}
		m.addNodeLocked(node, m.config.DefaultReplicas)
	}

	// ints 将整数切片按升序排序
	sort.Ints(m.keys)
	return nil
}

// Remove 移除节点
// func (m *Map) Remove(node string) error {
// 	if node == "" {
// 		return errors.New("invalid node")
// 	}

// 	m.mu.Lock()
// 	defer m.mu.Unlock()

// 	// 查出这个节点有多少个虚拟节点
// 	replicas := m.nodeReplicas[node]
// 	if replicas == 0 {
// 		return fmt.Errorf("node %s not found", node)
// 	}

// 	// 移除节点的所有虚拟节点
// 	for i := 0; i < replicas; i++ {
// 		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
// 		delete(m.hashMap, hash)
// 		for j := 0; j < len(m.keys); j++ {
// 			if m.keys[j] == hash {
// 				// 把前半段 + 后半段 接起来, 这个切片“打散成多个参数”，依次 append 进去
// 				m.keys = append(m.keys[:j], m.keys[j+1:]...)
// 				break
// 			}
// 		}
// 	}

// 	delete(m.nodeReplicas, node)
// 	delete(m.nodeCounts, node)
// 	return nil
// }

// Remove 移除节点（优化版：O(len(keys) + replicas) 而不是 O(replicas * len(keys))）
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.removeLocked(node)
}

// removeLocked 必须在写锁下调用
func (m *Map) removeLocked(node string) error {
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// 1) 删除 hashMap，并收集需要从 keys 删除的 hash（用 set）
	// map[int]struct{} 在 Go 里常用来当 set（集合）
	// key 是你要放进集合的元素（这里是虚拟节点的 hash 值）
	// value 用 struct{} 是因为它不占内存（空结构体大小为 0），只关心 key 是否存在
	removeSet := make(map[int]struct{}, replicas)
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))

		// hashMap 是 虚拟节点hash -> 真实节点
		// 当移除这个真实节点时，它对应的所有虚拟节点都不能再指向它了
		delete(m.hashMap, hash)
		removeSet[hash] = struct{}{}
	}

	// 2) 过滤 keys
	// m.keys[:0]：创建一个长度为 0，但容量仍然等于原切片容量的新切片
	// 也就是说：复用原来的底层数组，避免重新分配内存
	newKeys := m.keys[:0]
	for _, h := range m.keys {
		if _, ok := removeSet[h]; !ok {
			newKeys = append(newKeys, h)
		}
	}
	m.keys = newKeys

	// 3) 删除节点信息
	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node) // 注意：指针删除后仍可能被并发 Get 的本地变量引用，但指针本身仍有效，不会崩

	return nil
}

// Get 获取节点(一致性哈希的“顺时针第一个点”)
// func (m *Map) Get(key string) string {
// 	// 空 key 直接返回空
// 	if key == "" {
// 		return ""
// 	}

// 	m.mu.RLock()

// 	// 没有节点时直接返回空
// 	if len(m.keys) == 0 {
// 		m.mu.RUnlock()
// 		return ""
// 	}

// 	// key 先变成一个哈希值，再沿着环顺时针找到第一个“虚拟节点”，它所属的真实节点就是目标节点
// 	hash := int(m.config.HashFunc([]byte(key)))

// 	// 二分查找
// 	// 第一个满足 m.keys[i] >= hash 的下标 i，把它赋值给 idx
// 	// 如果没有这样的索引，搜索将返回 len(m.keys)
// 	idx := sort.Search(len(m.keys), func(i int) bool {
// 		return m.keys[i] >= hash
// 	})

// 	// 处理边界情况
// 	if idx == len(m.keys) {
// 		idx = 0
// 	}

// 	node := m.hashMap[m.keys[idx]]
// 	m.mu.RUnlock()

// 	m.mu.Lock()
// 	m.nodeCounts[node] = m.nodeCounts[node] + 1
// 	m.mu.Unlock()

// 	// 总请求数用 atomic，和 mu 解耦
// 	atomic.AddInt64(&m.totalRequests, 1)

// 	return node
// }

// Get 获取 key 对应的节点：
// 改进点：Get 路径不再对 nodeCounts 加写锁，只对计数器做 atomic++
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	// 1) 读锁只用于 ring 查找
	m.mu.RLock()
	if len(m.keys) == 0 {
		m.mu.RUnlock()
		return ""
	}

	// 二分查找
	// 第一个满足 m.keys[i] >= hash 的下标 i，把它赋值给 idx
	// 如果没有这样的索引，搜索将返回 len(m.keys)
	hash := int(m.config.HashFunc([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]

	// 拿到计数器指针（可能为 nil：例如 node 被移除/重建中，但 ring 查到 node 理论上应该存在）
	counterPtr := m.nodeCounts[node]
	m.mu.RUnlock()

	// 2) 计数用 atomic，不阻塞并发读
	if counterPtr != nil {
		atomic.AddInt64(counterPtr, 1)
	}
	atomic.AddInt64(&m.totalRequests, 1)

	return node
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	// 1. 样本量门槛：避免抖动
	// 不要因为统计数据太少，就频繁触发 rebalance（重建哈希环、调整虚拟节点数）。否则系统会出现来回调整、不断迁移 key、性能不稳定的现象，这就叫“抖动（thrashing/oscillation）”
	total := atomic.LoadInt64(&m.totalRequests)
	if total < 1000 {
		// 样本太少，不进行调整
		return
	}

	// 2. 读节点数量 & nodeCounts，需要用锁保护 map
	m.mu.RLock()
	nodeCount := len(m.nodeReplicas)
	if nodeCount == 0 {
		m.mu.RUnlock()
		return
	}

	// 计算负载情况
	// 计算理论平均负载
	avgLoad := float64(total) / float64(nodeCount)
	if avgLoad == 0 {
		m.mu.RUnlock()
		return
	}

	var maxDiff float64

	// for _, count := range m.nodeCounts {
	// 	diff := math.Abs(float64(count) - avgLoad)
	// 	ratio := diff / avgLoad // 相对偏差比例
	// 	if ratio > maxDiff {
	// 		maxDiff = ratio
	// 	}
	// }

	for node := range m.nodeReplicas {
		ptr := m.nodeCounts[node]
		var cnt int64
		if ptr != nil {
			cnt = atomic.LoadInt64(ptr)
		}
		diff := math.Abs(float64(cnt) - avgLoad)
		ratio := diff / avgLoad
		if ratio > maxDiff {
			maxDiff = ratio
		}
	}

	m.mu.RUnlock()

	// 3. 判断是否超过不均衡阈值
	if maxDiff <= m.config.LoadBalanceThreshold {
		return
	}

	// 4. 负载不均衡，调整虚拟节点
	m.rebalanceNodes()
}

// rebalanceNodes 根据统计负载调整 replicas，并重建哈希环
// 一个很重要的“例外”：你这个 rebalanceNodes() 会打破“只变一小段”的直觉
func (m *Map) rebalanceNodes() {
	// 独占整个 Map 的结构（keys / hashMap / nodeReplicas / nodeCounts）
	m.mu.Lock()
	defer m.mu.Unlock()

	// 没有节点就不用算了，避免除 0
	if len(m.nodeReplicas) == 0 {
		return
	}

	// 读取总请求数（之前是 atomic.AddInt64），这里用 atomic.Load 保持一致
	// total := atomic.LoadInt64(&m.totalRequests)
	total := m.totalRequests
	if total == 0 {
		// 没有请求，也没啥可平衡的
		return
	}

	// 理论上每个节点“应该”处理的平均请求数
	avgLoad := float64(total) / float64(len(m.nodeReplicas))
	if avgLoad == 0 {
		// 理论上 total>0 时 avgLoad 不会是 0，这里只是兜底
		return
	}

	// 先计算每个节点“应该”有多少虚拟节点，放在一个临时 map 里
	// 这样在这一步不会改动 nodeCounts / nodeReplicas，避免遍历时写 map
	newReplicas := make(map[string]int, len(m.nodeReplicas))

	// for node, currentReplicas := range m.nodeReplicas {
	// 	// 注意：这里从 nodeReplicas 遍历，而不是从 nodeCounts，
	// 	// 避免 Remove 之类的操作影响正在遍历的 map。
	// 	count := m.nodeCounts[node] // 如果没统计到就是 0
	// 	loadRatio := float64(count) / avgLoad

	// 	var replicas int
	// 	if loadRatio > 1 {
	// 		// 负载过高，减少虚拟节点
	// 		// 比如当前 100 个虚拟节点，loadRatio=2（负载是平均的 2 倍），新虚拟节点数 ≈ 50，减半
	// 		replicas = int(float64(currentReplicas) / loadRatio)
	// 	} else {
	// 		// 负载过低或刚好，增加一些虚拟节点
	// 		// loadRatio=1 → replicas=current
	// 		// loadRatio=0.5 → replicas≈1.5*current
	// 		// loadRatio=0   → replicas≈2*current
	// 		replicas = int(float64(currentReplicas) * (2 - loadRatio))
	// 	}

	// 	// 安全兜底：防止算出来 0 或负数
	// 	if replicas < 1 {
	// 		replicas = 1
	// 	}

	// 	// 限制在配置范围内
	// 	if replicas < m.config.MinReplicas {
	// 		replicas = m.config.MinReplicas
	// 	}
	// 	if replicas > m.config.MaxReplicas {
	// 		replicas = m.config.MaxReplicas
	// 	}

	// 	newReplicas[node] = replicas
	// }

	for node, currentReplicas := range m.nodeReplicas {
		ptr := m.nodeCounts[node]
		var cnt int64
		if ptr != nil {
			cnt = atomic.LoadInt64(ptr)
		}

		loadRatio := float64(cnt) / avgLoad

		var replicas int
		if loadRatio > 1 {
			// 比平均更忙：减少虚拟节点（让它分到更少 key）
			replicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 比平均更闲：增加虚拟节点（让它分到更多 key）
			replicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 安全兜底：防止算出来 0 或负数
		if replicas < 1 {
			replicas = 1
		}
		if replicas < m.config.MinReplicas {
			replicas = m.config.MinReplicas
		}
		if replicas > m.config.MaxReplicas {
			replicas = m.config.MaxReplicas
		}

		newReplicas[node] = replicas
	}

	// 用新的虚拟节点数量“重建”哈希环：
	// 1. 清空 keys 和 hashMap
	// 2. 清空 nodeReplicas
	// 3. 再用 addNode 按 newReplicas 重建
	// m.keys = nil
	// m.hashMap = make(map[int]string, len(newReplicas)*m.config.MaxReplicas)
	// m.nodeReplicas = make(map[string]int, len(newReplicas))

	// 重建：清空 keys/hashMap/nodeReplicas，再按 newReplicas 加回去
	m.keys = m.keys[:0]
	m.hashMap = make(map[int]string, len(newReplicas)*m.config.DefaultReplicas)
	m.nodeReplicas = make(map[string]int, len(newReplicas))

	for node, replicas := range newReplicas {
		// addNode 会：
		// - 按 node / i 生成虚拟节点 hash
		// - 填充 m.keys / m.hashMap
		// - 更新 m.nodeReplicas[node] = replicas
		// m.addNode(node, replicas)

		// 注意：nodeCounts 指针 map 不清空（保留计数器对象），只重置数值
		m.addNodeLocked(node, replicas)
	}

	// 重置统计：从这次重平衡之后重新开始采样
	// for node := range m.nodeCounts {
	// 	m.nodeCounts[node] = 0
	// }

	// 重置统计（从这次重建后重新采样）
	for node := range m.nodeCounts {
		ptr := m.nodeCounts[node]
		if ptr != nil {
			atomic.StoreInt64(ptr, 0)
		}
	}

	atomic.StoreInt64(&m.totalRequests, 0)

	// 最后把虚拟节点位置排个序，保证 Get 里的二分查找正常
	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
// 按节点返回当前各节点的负载占比（每个节点处理的请求数 / 总请求数），用于观察一致性哈希的负载分布情况。
// func (m *Map) GetStats() map[string]float64 {
// 	m.mu.RLock()
// 	defer m.mu.RUnlock()

// 	stats := make(map[string]float64)
// 	total := atomic.LoadInt64(&m.totalRequests)
// 	if total == 0 {
// 		return stats
// 	}

// 	for node, count := range m.nodeCounts {
// 		// 该节点请求数占总请求数的比例
// 		stats[node] = float64(count) / float64(total)
// 	}
// 	return stats
// }

// GetStats 获取负载统计信息（每个节点请求占比）
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)

	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node := range m.nodeReplicas {
		ptr := m.nodeCounts[node]
		var cnt int64
		if ptr != nil {
			cnt = atomic.LoadInt64(ptr)
		}
		stats[node] = float64(cnt) / float64(total)
	}

	return stats
}
