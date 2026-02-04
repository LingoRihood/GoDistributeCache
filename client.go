/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.12 9:47
* Client 就是一个“远程节点的代理”，实现 Peer 接口，用 gRPC 去访问别的缓存节点
***************************************************************/

package gocache

import (
	"context"
	"fmt"
	"time"

	pb "github.com/LingoRihood/GoDistributeCache/pb"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	addr    string           // 要连接的远程缓存节点地址（比如 10.0.0.2:8000）
	svcName string           // 服务名（比如 go-cache），用于从 etcd 的 /services/{svcName}/... 查节点
	etcdCli *clientv3.Client // etcd 客户端，用于 服务发现
	conn    *grpc.ClientConn // gRPC 连接对象，底层有连接池、HTTP2、keepalive 等资源
	grpcCli pb.GoCacheClient // 真正跟远程 gRPC 服务打交道的对象, protobuf 自动生成的 gRPC Stub（“远程接口的本地代理”），你调用它就像调用本地方法一样，它会帮你发 RPC
}

// *Client 必须实现 Peer 接口里的所有方法，否则编译时报错
/*
(*Client)(nil) 是一个 *Client 类型的 nil 指针；
赋值给一个类型为 Peer 的变量；
如果 *Client 没有实现 Peer 接口里定义的所有方法，编译器会直接报错。
*/
var _ Peer = (*Client)(nil)

// 创建一个能跟远程 GoCache 节点通信的客户端 *Client
func NewClient(addr string, svcName string, etcdCli *clientv3.Client) (*Client, error) {
	var err error
	createdEtcd := false // 标记 etcdCli 是不是在这里新建的，方便出错时关闭

	// 1. 如果没有传 etcdCli，就按默认配置新建一个
	if etcdCli == nil {
		// etcd = 通讯录/服务目录（发现节点用）
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %v", err)
		}
		createdEtcd = true
	}

	// 2. 如果 addr 为空，根据 svcName 从 etcd 里做一次服务发现
	if addr == "" {
		keyPrefix := fmt.Sprintf("/services/%s/", svcName)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		resp, err := etcdCli.Get(ctx, keyPrefix, clientv3.WithPrefix())
		if err != nil {
			if createdEtcd {
				// 如果是你自己创建的：函数出错时应该关闭它（避免泄漏）
				// 如果是外面传进来的：不能随便 Close（那是调用方的资源）
				_ = etcdCli.Close()
			}
			return nil, fmt.Errorf("failed to discover service from etcd: %v", err)
		}

		// 说明 etcd 里没有注册任何节点，当然没法连
		if len(resp.Kvs) == 0 {
			if createdEtcd {
				_ = etcdCli.Close()
			}
			return nil, fmt.Errorf("no instances found for service %q", svcName)
		}

		// 简单起见：先用第一个实例
		// key: /services/go-cache/10.0.0.1:8000 value: 10.0.0.1:8000
		// key: /services/go-cache/10.0.0.2:8000 value: 10.0.0.2:8000
		// 简单选了第一个节点（不是一致性哈希/负载均衡，只是“能用就行”的策略）
		addr = string(resp.Kvs[0].Value)
		logrus.Infof("discovered %s instance at %s", svcName, addr)
	}

	// 3. 使用 grpc.NewClient 创建连接（替代 Dial / DialContext）
	// 建一条到服务地址的 gRPC 连接, gRPC = 打电话的方式（远程调用用）
	// 现在我知道要打给谁了（addr），我建立一条电话线（conn）
	conn, err := grpc.NewClient(
		addr,
		// 不用 TLS，加密关掉（明文）
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		// 如果当前连接暂时不可用，不要立刻返回错误，可以等一会儿，等连上再发请求
		// 如果现在对方暂时没准备好（连接没立刻成功），不要立刻失败
		// 允许 gRPC 在一段时间内等它就绪再发请求（具体等多久取决于每次 RPC 的 ctx deadline）
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	if err != nil {
		if createdEtcd {
			// 出错时清理 etcd
			_ = etcdCli.Close()
		}
		return nil, fmt.Errorf("failed to create gRPC client to %s: %v", addr, err)
	}

	// 用这条连接生成 protobuf 的 gRPC 客户端
	// protobuf：负责“请求/响应怎么定义与序列化”
	// 用 conn 生成业务客户端 grpcCli（拨号器）
	/*
		conn 是“电话线”
		grpcCli 是“拨号器/业务接口”，里面有 Get/Set/Delete 这些方法
		调用 grpcCli.Get(ctx, req) 就会通过 conn 发到远端 server
	*/
	grpcClient := pb.NewGoCacheClient(conn)

	client := &Client{
		addr:    addr,
		svcName: svcName,
		etcdCli: etcdCli,
		conn:    conn,
		grpcCli: grpcClient,
	}

	return client, nil
}

// 向远程节点取缓存
func (c *Client) Get(group, key string) ([]byte, error) {
	// 创建 3 秒超时的 context
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	// 清理 context 内部可能持有的一些资源，防止“context 泄漏”
	defer cancel()

	// 发起 gRPC 的 Get 调用
	/*
		把 pb.Request 序列化成二进制；
		通过 conn 发送到远程服务的 GoCache.Get 方法；
		等待服务端处理完，返回一个 pb.ResponseForGet 或 error
		请求体是 protobuf 的 pb.Request
		响应 resp 也是 protobuf 结构体
	*/
	resp, err := c.grpcCli.Get(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from kamacache: %v", err)
	}

	// resp.GetValue() 是 protobuf 自动生成的 getter 方法，等价于 resp.Value，类型是 []byte
	return resp.GetValue(), nil
}

// 在 3 秒超时限制内，通过 gRPC 请求远程节点，在远端节点上，删除“某个 group（缓存分组）里的某个 key”
// RPC 成功就返回服务端给的 bool；失败就返回 error
func (c *Client) Delete(group, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Delete(ctx, &pb.Request{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from kamacache: %v", err)
	}

	return resp.GetValue(), nil
}

// 远程写入 key/value
func (c *Client) Set(ctx context.Context, group, key string, value []byte) error {
	resp, err := c.grpcCli.Set(ctx, &pb.Request{
		Group: group,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return fmt.Errorf("failed to set value to kamacache: %v", err)
	}
	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

// 把 gRPC 连接关掉，避免资源泄漏
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
