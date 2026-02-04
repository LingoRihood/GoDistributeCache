/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.9 16:26
***************************************************************/

package registry

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	// 「起了别名的 import」导入 etcd 的 v3 版本 Go 客户端库，并在当前文件里用 clientv3 这个名字来引用它
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config 定义etcd客户端配置
type Config struct {
	Endpoints   []string      // 集群地址
	DialTimeout time.Duration // 连接超时时间
}

// DefaultConfig 提供默认配置
var DefaultConfig = &Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
}

// Register 注册服务到etcd
// Register(ctx, svcName, addr) 会把当前节点地址写进 etcd（作为服务发现的“在线名单”），
// 并且用租约 + 心跳让它一直保持在线；当 ctx 结束时主动撤销租约，服务自动下线。
// 可以把 etcd 想成群聊公告栏/通讯录：
// 在线的人会把自己的联系方式贴上去
// 离线的人（不续租）会自动被公告栏删掉
// func Register(svcName, addr string, stopCh <-chan error) error {
func Register(ctx context.Context, svcName, addr string) error {
	// 创建 etcd 客户端
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   DefaultConfig.Endpoints,
		DialTimeout: DefaultConfig.DialTimeout,
	})
	// 从服务进程 → 去连 etcd 服务器
	// 这一步没有成功连上 etcd 集群
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 出错或 ctx 结束时统一关闭
	cleanup := func() {
		if err := cli.Close(); err != nil {
			logrus.Warnf("close etcd client error: %v", err)
		}
	}

	// 处理服务地址：如果只给了端口，自动补上本机 IP
	// 为了避免传 ":8001" 这种地址时，写进 etcd 的信息没法被别的机器访问
	localIP, err := getLocalIP()
	if err != nil {
		// 是用于关闭 etcd 客户端连接的操作
		cleanup()
		return fmt.Errorf("failed to get local IP: %v", err)
	}

	if addr == "" {
		cleanup()
		return fmt.Errorf("addr is empty")
	}

	// 如果 addr 是 ":8001"，改成 "192.168.1.100:8001"
	if addr[0] == ':' {
		addr = fmt.Sprintf("%s%s", localIP, addr)
	}

	// 3. 创建租约（带超时）
	/*
		以外部传入的 ctx 为父 ctx，创建一个 3 秒超时的子 ctx：leaseCtx 是从 ctx 派生出来的子 context
		3 秒内若 Grant 没返回，就会自动超时取消；
		外部一旦 ctx 被取消，leaseCtx 也会跟着取消
	*/
	leaseCtx, cancelLease := context.WithTimeout(ctx, 3*time.Second)
	/*
		向 etcd 申请一个租约，TTL = 10 秒。
		租约 ID 存在 lease.ID 里面。
	*/
	// 这一步要经过网络 → etcd → 返回结果
	// 没有 WithTimeout，就不存在“Grant 超时”这件事（除非你外层 ctx 本来就会超时/取消）。
	// 它只会一直等：要么成功返回，要么遇到错误返回，要么 ctx 被取消
	lease, err := cli.Grant(leaseCtx, 10) // TTL = 10 秒

	// Grant 返回后，这个 leaseCtx 已经没用了，及时 cancel()：防止 context 泄露
	cancelLease()
	if err != nil {
		cleanup()
		return fmt.Errorf("failed to create lease: %w", err)
	}

	// 4. 写入服务信息（带超时）
	// 构造 key，比如：/services/go-cache/192.168.1.100:8080
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)

	// 写入服务信息（Put）：把自己贴到通讯录，并绑定租约
	putCtx, cancelPut := context.WithTimeout(ctx, 3*time.Second)

	// 把 key -> addr 这条记录写入 etcd；并绑定到刚刚创建的租约 lease.ID
	// 往 etcd 写一条记录：key 表示我是谁，value 表示我的地址，并且把它绑定到租约上。
	// 租约过期 → 这条 key 自动删掉（自动下线）
	_, err = cli.Put(putCtx, key, addr, clientv3.WithLease(lease.ID))
	cancelPut()
	if err != nil {
		cleanup()
		return fmt.Errorf("failed to put key-value to etcd: %w", err)
	}

	// 5. 启动租约续期（KeepAlive）
	// kaCtx 继承自外部 ctx：外部 ctx 结束（cancel/timeout）→ kaCtx 也结束 → 续租停止
	kaCtx, cancelKA := context.WithCancel(ctx)

	// 启动一个后台心跳机制，定期向 etcd 发送续约请求，让 lease 一直存活
	// keepAliveCh 是一个 channel，会不断收到续租结果（resp）
	keepAliveCh, err := cli.KeepAlive(kaCtx, lease.ID)
	if err != nil {
		cancelKA()
		cleanup()
		return fmt.Errorf("failed to keep lease alive: %w", err)
	}

	// 6. 后台处理续期 & 下线:后台 goroutine：处理续租反馈 + 退出时主动 Revoke
	go func() {
		defer func() {
			// cancelKA()：停止 keepalive
			// 停止租约续租（KeepAlive）
			// 让 etcd 客户端不再继续发心跳，不再占用 goroutine/连接/资源
			cancelKA()
			// 关闭 etcd client（cli.Close），释放底层连接、goroutine、资源。
			cleanup()
		}()

		for {
			select {
			// 当外面调用 cancel() 或 ctx 超时时
			// 服务准备退出 → 主动下线
			case <-ctx.Done():
				// ctx 结束 → 主动撤销租约，下线服务
				// 创建一个新的 context：revokeCtx，最多等 3 秒
				// 为什么不用原来的 ctx，而用 context.Background()？
				// 因为此刻 ctx 已经 Done 了，用它会立刻取消，Revoke 可能来不及发出去
				// 所以新建一个背景 ctx + 3 秒超时，保证有机会把撤销请求发出去
				revokeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				/*
					显式撤销这个租约；
					绑定在该租约上的所有 key 都会被 etcd 删除；
					也就是服务从 etcd 中注销。
				*/
				if _, err := cli.Revoke(revokeCtx, lease.ID); err != nil {
					logrus.Warnf("failed to revoke lease %d: %v", lease.ID, err)
				}
				// cancel() 释放 revokeCtx
				cancel()
				return

			// 打印续租日志
			case resp, ok := <-keepAliveCh:
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s", svcName, addr)
	return nil
}

func getLocalIP() (string, error) {
	// 获取本机所有网络地址
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		// *net.IPNet 代表“IP 地址 + 子网掩码”
		// ipNet.IP.IsLoopback() 判断这个 IP 是不是回环地址（比如 127.0.0.1）
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// 只要 IPv4 地址
			if ipNet.IP.To4() != nil {
				// 找到第一个满足条件的 IPv4 地址，就直接返回字符串形式，比如 "192.168.1.100"，并且 error == nil
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}

/*
func Register(svcName, addr string, stopCh <-chan error) error {
	// 创建 etcd 客户端
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   DefaultConfig.Endpoints,
		DialTimeout: DefaultConfig.DialTimeout,
	})
	// 从你的服务进程 → 去连 etcd 服务器
	// 这一步没有成功连上 etcd 集群
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 处理服务地址：如果只给了端口，自动补上本机 IP
	localIP, err := getLocalIP()
	if err != nil {
		// 是用于关闭 etcd 客户端连接的操作
		cli.Close()
		return fmt.Errorf("failed to get local IP: %v", err)
	}

	if len(addr) > 0 && addr[0] == ':' {
		addr = fmt.Sprintf("%s%s", localIP, addr)
	}

	// 创建租约
	// cli.Grant(ctx, ttlSeconds)：向 etcd 申请一个租约，租约时间 TTL = 10 秒
	// 传入一个上下文 ctx，用来控制这次请求的生命周期
	// context.Background(): 创建一个最顶层、空的、不会自动取消的上下文
	// 你把服务信息 (/services/...) 绑定到这个租约上。
	// 如果你的服务挂了，没有继续发送 KeepAlive（心跳），租约过期后，etcd 会自动删除这个 key。
	// 这样服务下线是自动的，其他组件发现某个服务不再存在，就知道它已经不可用了
	lease, err := cli.Grant(context.Background(), 10) // 增加租约时间到10秒
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to create lease: %v", err)
	}

	// 注册服务，使用完整的key路径
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	// clientv3.WithLease(lease.ID): 表示这个 key 绑定到了刚才创建的租约上,租约一旦过期，这个 key 就会被 etcd 自动删除
	_, err = cli.Put(context.Background(), key, addr, clientv3.WithLease(lease.ID))
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v", err)
	}

	// 启动租约续期（KeepAlive）
	keepAliveCh, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	// 处理租约续期和服务注销
	go func() {

		// 当这个 goroutine 退出时，自动关闭 etcd 客户端连接。
		// 退出的两种情况：
		// 	接收到 stopCh 信号（手动下线）。
		// 	keepAliveCh 被关闭（心跳中断，可能是 etcd 或网络问题）
		defer cli.Close()
		for {
			select {
			case <-stopCh:
				// 服务注销，撤销租约: 用 WithTimeout 限制撤销操作最多等待 3 秒
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				// 租约被撤销 → 绑定这个租约的所有 key 都会被 etcd 删除
				cli.Revoke(ctx, lease.ID)
				cancel()
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s", svcName, addr)
	return nil
}
*/
