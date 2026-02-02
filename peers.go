/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.12 14:57
***************************************************************/

package gocache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/LingoRihood/GoDistributeCache/consistenthash"
	"github.com/LingoRihood/GoDistributeCache/registry"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "go-cache"

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// client.go：封装 gRPC Client，作为“远端节点代理”，实现 Peer 接口
// Peer 定义了缓存节点的接口
type Peer interface {
	Get(group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(group string, key string) (bool, error)
	Close() error
}

// ClientPicker 实现了PeerPicker接口
type ClientPicker struct {
	selfAddr string              // 自己的地址（用于避免把自己当远端）
	svcName  string              // 服务名，用于拼 etcd 的 key 前缀
	mu       sync.RWMutex        // 保护 consHash/clients 的并发读写
	consHash *consistenthash.Map // 一致性哈希环
	clients  map[string]*Client  // addr -> gRPC client
	etcdCli  *clientv3.Client    // etcd client
	ctx      context.Context     // 控制后台协程生命周期
	cancel   context.CancelFunc  // Close() 时取消 ctx
}

// PickerOption 定义配置选项
type PickerOption func(*ClientPicker)

// WithServiceName 设置服务名称
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// PrintPeers 打印当前已发现的节点（仅用于调试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建新的ClientPicker实例
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	// 创建一个可取消的上下文：用于后续后台协程
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr: addr,
		svcName:  defaultSvcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		ctx:      ctx,
		cancel:   cancel,
	}

	for _, opt := range opts {
		opt(picker)
	}

	// 创建 etcd 客户端，保存到 picker 里
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   registry.DefaultConfig.Endpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建 etcd 客户端，并挂在 picker.etcdCli 上
	picker.etcdCli = cli

	// 启动服务发现
	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	return picker, nil
}

// startServiceDiscovery 启动服务发现
// 先从 etcd 把当前所有服务节点“全部拉一遍”（全量）；
// 然后再起一个 goroutine 持续监听后面的变化（增量）
func (p *ClientPicker) startServiceDiscovery() error {
	// 先进行全量更新
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	// 启动增量更新
	go p.watchServiceChanges()
	return nil
}

// fetchAllServices 获取所有服务实例
func (p *ClientPicker) fetchAllServices() error {
	// p.ctx 是在 NewClientPicker 里创建、保存的那个 context，代表整个 ClientPicker 的生命周期
	// 以 p.ctx 为父 context，再套一层“最多 3 秒”的超时
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	// 把所有 /services/go-cache/* 的 key 都查出来
	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName+"/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// resp.Kvs 就是所有符合条件的 key-value 对
	for _, kv := range resp.Kvs {
		// etcd 里的 Value 是 []byte 类型，这里转成 string
		/*
			resp.Kvs 是一个切片，里面每个元素就是一个 *mvccpb.KeyValue
			对于第一条：
			kv.Key = []byte("/services/go-cache/10.0.0.1:8000")
			kv.Value = []byte("10.0.0.1:8000")
			对于第二条：
			kv.Key = []byte("/services/go-cache/10.0.0.2:8000")
			kv.Value = []byte("10.0.0.2:8000")
		*/
		addr := string(kv.Value)
		if addr != "" && addr != p.selfAddr {
			// 不把“自己这个节点”当成远程节点加入
			p.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	// 基于这个 etcd 客户端，创建一个“watcher 对象”
	watcher := clientv3.NewWatcher(p.etcdCli)

	// etcd 里面只要有对应 key 的变化，它就会往这个 channel 里塞一条 WatchResponse
	// 从现在开始，帮我监听 /services/{svcName} 这个前缀下所有 key 的变化，一旦有变化，把事件塞到 watchChan 这个管道里。
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		// 当外面调用了 p.cancel()（通常在 picker.Close() 里）；或者 p.ctx 因为父 context 被取消；这个 channel 就会被关闭；<-p.ctx.Done() 就会立刻收到信号
		case <-p.ctx.Done():
			watcher.Close()
			return
		// 收到 etcd 的变更事件
		case resp := <-watchChan:
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		addr := string(event.Kv.Value)
		// 如果这条事件是关于我自己的地址，那就忽略，不当成远程节点来处理
		if addr == p.selfAddr {
			continue
		}

		switch event.Type {
		// 处理新增 / 更新：EventTypePut
		case clientv3.EventTypePut:
			if _, exists := p.clients[addr]; !exists {
				p.set(addr)
				logrus.Infof("New service discovered at %s", addr)
			}
		case clientv3.EventTypeDelete:
			if client, exists := p.clients[addr]; exists {
				// 关闭这个节点对应的 gRPC 连接
				client.Close()
				p.remove(addr)
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

// set 添加服务实例
// 为某个节点地址创建客户端并加入哈希环
func (p *ClientPicker) set(addr string) {
	if client, err := NewClient(addr, p.svcName, p.etcdCli); err == nil {
		p.consHash.Add(addr)
		p.clients[addr] = client
		logrus.Infof("Successfully created client for %s", addr)
	} else {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
	}
}

// remove 移除服务实例
// 从哈希环和 clients 中移除节点
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.clients, addr)
}

// PickPeer 选择peer节点
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 根据 key 选一个节点
	if addr := p.consHash.Get(key); addr != "" {
		if client, ok := p.clients[addr]; ok {
			return client, true, addr == p.selfAddr
		}
	}
	return nil, false, false
}

// Close 关闭所有资源
func (p *ClientPicker) Close() error {
	// 先通知所有后台服务发现的协程停止工作
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()

	var errs []error

	// 关闭所有远程客户端连接
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	// 关闭 etcd 客户端
	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}

	return nil
}

// parseAddrFromKey 从etcd key中解析地址
func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)

	// 判断 key 是否是这个前缀
	if strings.HasPrefix(key, prefix) {
		// 假如etcd 的 key 是/services/go-cache/10.0.0.2:8000
		// 这两步做的就是：
		// HasPrefix(key, "/services/go-cache/") → true
		// TrimPrefix(key, "/services/go-cache/") → "10.0.0.2:8000"
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}
