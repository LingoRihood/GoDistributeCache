/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.10 17:04
* 对外提供 gRPC 接口（Get/Set/Delete）——别人通过网络来访问你这台机器上的缓存。把自己注册到 etcd——让其他节点能“发现”你
***************************************************************/

package gocache

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"crypto/tls"

	pb "github.com/LingoRihood/GoDistributeCache/pb"
	"github.com/LingoRihood/GoDistributeCache/registry"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Server 定义缓存服务器
type Server struct {
	// 让 Server 自动实现 gRPC 生成的 GoCacheServer 接口；给所有 RPC 提供一个默认“未实现”的基础实现
	pb.UnimplementedGoCacheServer
	addr       string           // 服务地址
	svcName    string           // 服务名称
	groups     *sync.Map        // 缓存组
	grpcServer *grpc.Server     // 真正的 gRPC 服务实例
	etcdCli    *clientv3.Client // etcd 客户端，给服务注册/发现用
	stopCh     chan error       // 停止信号
	opts       ServerOptions    // 服务器选项
}

// ServerOptions 服务器配置选项
type ServerOptions struct {
	// etcd 集群地址列表，比如：[]string{"127.0.0.1:2379","127.0.0.1:2380"}
	EtcdEndpoints []string      // etcd端点, 告诉它连哪个 etcd 集群（“通讯录”）
	DialTimeout   time.Duration // 连 etcd 的超时
	MaxMsgSize    int           // 最大消息大小, gRPC 单条消息最大接收大小（避免超大请求）
	TLS           bool          // 是否启用TLS
	CertFile      string        // 证书文件
	KeyFile       string        // 密钥文件
}

// DefaultServerOptions 默认配置
// var DefaultServerOptions = &ServerOptions{
// 	EtcdEndpoints: []string{"localhost:2379"},
// 	DialTimeout:   5 * time.Second,

// 	// 1MB = 1024 KB = 1048576 字节
// 	// 4 << 20 = 4 * 1048576 = 4194304
// 	// 使用左移运算（<<）进行乘法运算，尤其是在处理 2 的幂次方时，可以更高效地进行计算，因为计算机硬件优化了这种操作
// 	MaxMsgSize: 4 << 20, // 4MB
// }

// DefaultServerOptions 默认配置（注意：值，不是 *指针）
var DefaultServerOptions = ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// loadTLSCredentials 加载TLS证书
// 给我证书和私钥文件路径 → 我帮你造好一份 gRPC 可以直接用的 TLS 凭证对象。
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	// 加载证书和私钥
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// 包装成 gRPC 能用的 TransportCredentials
	return credentials.NewTLS(&tls.Config{
		// 这个 server（或 client）用 cert 这张证书来完成 TLS 握手
		Certificates: []tls.Certificate{cert},
	}), nil
}

// WithTLS 设置TLS配置
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// NewServer 创建新的服务器实例
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	// options := DefaultServerOptions
	// for _, opt := range opts {
	// 	opt(options)
	// }

	// ✅ 拷贝默认配置（值拷贝），不会污染全局默认值
	options := DefaultServerOptions
	for _, opt := range opts {
		opt(&options)
	}

	// 创建etcd客户端, 读取配置，建立 etcd 连接
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建 gRPC Server，并根据配置添加选项
	var serverOpts []grpc.ServerOption

	// 防止有人发超大 payload 造成内存/性能风险
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	// 如果配置了 TLS，就加载证书
	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:    addr,
		svcName: svcName,
		groups:  &sync.Map{},

		// serverOpts...：把这个切片拆开，当成多个参数传进去，相当于：
		// grpc.NewServer(serverOpts[0], serverOpts[1], serverOpts[2], ...)
		// 函数定义里：opts ...ServerOption
		// 函数调用时：NewServer(addr, svcName, serverOpts...)
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       options,
	}

	// 1. 注册业务服务（缓存服务）
	// 把你的 srv 挂到 srv.grpcServer 上，告诉 gRPC：以后凡是 GoCache 的 RPC 请求，就调用 srv.Get / srv.Set / srv.Delete
	pb.RegisterGoCacheServer(srv.grpcServer, srv)

	// 2. 创建健康检查服务并注册到同一个 gRPC server
	// 这个服务专门响应一个标准的 RPC：Check，用来查询当前服务是不是健康
	healthServer := health.NewServer()

	// 把刚刚那个 healthServer 注册到同一个 gRPC 服务器 srv.grpcServer 上
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)

	// 3. 标记这个服务名当前是“健康可用”的
	// HealthCheckResponse_SERVING：表示“正在正常提供服务”
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// Start 启动服务器
// func (s *Server) Start() error {
// 	// 启动gRPC服务器
// 	lis, err := net.Listen("tcp", s.addr)
// 	if err != nil {
// 		return fmt.Errorf("failed to listen: %v", err)
// 	}

// 	// 注册到etcd
// 	stopCh := make(chan error)
// 	go func() {
// 		if err := registry.Register(s.svcName, s.addr, stopCh); err != nil {
// 			logrus.Errorf("failed to register service: %v", err)
// 			close(stopCh)
// 			return
// 		}
// 	}()

// 	logrus.Infof("Server starting at %s", s.addr)
// 	return s.grpcServer.Serve(lis)
// }

// Start 启动服务器
func (s *Server) Start(ctx context.Context) error {
	// 启动 gRPC 监听 监听端口, 在本机打开一个 TCP 端口，等待客户端来连。
	// 监听成功返回一个 net.Listener（这里叫 lis）
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 启动一个 goroutine，在后台把服务 svcName 的 addr 注册到 etcd 里去，生命周期由 ctx 控制
	// 注册到 etcd（用 ctx 控制生命周期）
	go func() {
		if err := registry.Register(ctx, s.svcName, s.addr); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			// 注册失败这里先只打日志，是否要强制退出看你自己需求
		}
	}()

	// 打一条日志说明服务要启动了
	logrus.Infof("Server starting at %s", s.addr)

	// 在 lis 这个监听器上接收客户端连接；为每个连接读取 gRPC 请求
	// 会开始接受客户端连接、执行 RPC 函数，这就是“服务器开始工作”的时刻
	return s.grpcServer.Serve(lis)
}

// Stop 停止服务器
// 收到退出信号（Ctrl+C、SIGTERM）的时候调用
func (s *Server) Stop() {
	// 优雅停止 gRPC server
	// 不再接受新的连接和请求；让正在处理的 RPC 调用慢慢跑完；等这些都处理完，再真正关闭；
	// 像“先把正在排队和结账的顾客处理完，然后关门”。
	s.grpcServer.GracefulStop()

	// 关闭在 NewServer 里创建的 etcdCli（这个是你自己留着用的）
	// 关闭 etcd 客户端
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

// Stop 停止服务器
// func (s *Server) Stop() {
// 	close(s.stopCh)
// 	s.grpcServer.GracefulStop()
// 	if s.etcdCli != nil {
// 		s.etcdCli.Close()
// 	}
// }

// Get：从某个缓存分组里读一个 key
// Get 实现Cache服务的Get方法
func (s *Server) Get(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	// 从某个缓存分组里读一个 key
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return nil, err
	}

	// 构造响应
	return &pb.ResponseForGet{Value: view.ByteSLice()}, nil
}

// Set 实现Cache服务的Set方法
// Set：向缓存写入一个 key
func (s *Server) Set(ctx context.Context, req *pb.Request) (*pb.ResponseForGet, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	// 从 context 中获取标记，如果没有则创建新的 context
	// 从 ctx 里读一个值：
	// 如果以前有人用 context.WithValue 写过这个 key，就能拿到那个值；
	// 如果没人写过，就会返回 nil
	// fromPeer := ctx.Value("from_peer")
	// if fromPeer == nil {
	// 	ctx = context.WithValue(ctx, "from_peer", true)
	// }

	// 这是 peer->peer 的同步请求入口：直接标记来源，避免 SA1029（不要用 string 当 key）
	// 这个标记是为了防止“节点A同步给B，B又同步回A，死循环”
	/*
		如果 不是来自 peer 的请求（比如用户直接写本机）
		→ 才需要同步到其他节点（syncToPeers）

		如果 是来自 peer 的请求（说明这是别人同步过来的）
		→ 绝对不能再同步出去，否则会形成环
	*/
	ctx = context.WithValue(ctx, fromPeerKey, true)

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.ResponseForGet{Value: req.Value}, nil
}

// Delete：删除 key
// Delete 实现Cache服务的Delete方法
func (s *Server) Delete(ctx context.Context, req *pb.Request) (*pb.ResponseForDelete, error) {
	group := GetGroup(req.Group)
	if group == nil {
		return nil, fmt.Errorf("group %s not found", req.Group)
	}

	err := group.Delete(ctx, req.Key)
	return &pb.ResponseForDelete{Value: err == nil}, err
}
