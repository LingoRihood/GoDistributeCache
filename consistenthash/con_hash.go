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
	mu            sync.RWMutex     // 读写锁，保证并发安全
	config        *Config          // 配置信息
	keys          []int            // 哈希环上的所有虚拟节点位置，按顺序排列
	hashMap       map[int]string   // 从哈希值到实际节点名称的映射
	nodeReplicas  map[string]int   // 每个实际节点对应的虚拟节点数量
	nodeCounts    map[string]int64 // 记录每个节点处理的请求数
	totalRequests int64            // 记录总请求数，用于负载均衡计算
}

// Option 配置选项
type Option func(*Map)

// New 创建一致性哈希实例
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]int64),
	}

	for _, opt := range opts {
		opt(m)
	}

	// 后台 Rebalance 协程
	m.startBalancer() // 启动负载均衡器
	return m
}

// 将checkAndRebalance移到单独的goroutine中
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for range ticker.C {
			m.checkAndRebalance()
		}
	}()
}

// WithConfig 设置配置
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// addNode 添加节点的虚拟节点
func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		// 生成每个虚拟节点的 hash 值
		// node = "10.0.0.1:6379"，i = 0 → "10.0.0.1:6379-0"
		// node = "10.0.0.1:6379"，i = 1 → "10.0.0.1:6379-1"
		// 把字符串转换成字节切片，传给哈希函数
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
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
		m.addNode(node, m.config.DefaultReplicas)
	}

	// ints 将整数切片按升序排序
	sort.Ints(m.keys)
	return nil
}

// Remove 移除节点
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 查出这个节点有多少个虚拟节点
	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// 移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc([]byte(fmt.Sprintf("%s-%d", node, i))))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				// 把前半段 + 后半段 接起来, 这个切片“打散成多个参数”，依次 append 进去
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// Get 获取节点
func (m *Map) Get(key string) string {
	// 空 key 直接返回空
	if key == "" {
		return ""
	}

	m.mu.RLock()

	// 没有节点时直接返回空
	if len(m.keys) == 0 {
		m.mu.RUnlock()
		return ""
	}

	// key 先变成一个哈希值，再沿着环顺时针找到第一个“虚拟节点”，它所属的真实节点就是目标节点
	hash := int(m.config.HashFunc([]byte(key)))

	// 二分查找
	// 第一个满足 m.keys[i] >= hash 的下标 i，把它赋值给 idx
	// 如果没有这样的索引，搜索将返回 len(m.keys)
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	m.mu.RUnlock()

	m.mu.Lock()
	m.nodeCounts[node] = m.nodeCounts[node] + 1
	m.mu.Unlock()

	// 总请求数用 atomic，和 mu 解耦
	atomic.AddInt64(&m.totalRequests, 1)

	return node
}

// checkAndRebalance 检查并重新平衡虚拟节点
func (m *Map) checkAndRebalance() {
	// 1. 样本量检查（用 atomic 读）
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

	for _, count := range m.nodeCounts {
		diff := math.Abs(float64(count) - avgLoad)
		ratio := diff / avgLoad // 相对偏差比例
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

// rebalanceNodes 重新平衡节点
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

	for node, currentReplicas := range m.nodeReplicas {
		// 注意：这里从 nodeReplicas 遍历，而不是从 nodeCounts，
		// 避免 Remove 之类的操作影响正在遍历的 map。
		count := m.nodeCounts[node] // 如果没统计到就是 0
		loadRatio := float64(count) / avgLoad

		var replicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			// 比如当前 100 个虚拟节点，loadRatio=2（负载是平均的 2 倍），新虚拟节点数 ≈ 50，减半
			replicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低或刚好，增加一些虚拟节点
			// loadRatio=1 → replicas=current
			// loadRatio=0.5 → replicas≈1.5*current
			// loadRatio=0   → replicas≈2*current
			replicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 安全兜底：防止算出来 0 或负数
		if replicas < 1 {
			replicas = 1
		}

		// 限制在配置范围内
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
	m.keys = nil
	m.hashMap = make(map[int]string, len(newReplicas)*m.config.MaxReplicas)
	m.nodeReplicas = make(map[string]int, len(newReplicas))

	for node, replicas := range newReplicas {
		// addNode 会：
		// - 按 node / i 生成虚拟节点 hash
		// - 填充 m.keys / m.hashMap
		// - 更新 m.nodeReplicas[node] = replicas
		m.addNode(node, replicas)
	}

	// 重置统计：从这次重平衡之后重新开始采样
	for node := range m.nodeCounts {
		m.nodeCounts[node] = 0
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 最后把虚拟节点位置排个序，保证 Get 里的二分查找正常
	sort.Ints(m.keys)
}

// GetStats 获取负载统计信息
// 按节点返回当前各节点的负载占比（每个节点处理的请求数 / 总请求数），用于观察一致性哈希的负载分布情况。
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, count := range m.nodeCounts {
		// 该节点请求数占总请求数的比例
		stats[node] = float64(count) / float64(total)
	}
	return stats
}
