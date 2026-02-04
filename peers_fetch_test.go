package gocache

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/LingoRihood/GoDistributeCache/consistenthash"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// 启一个“假 gRPC Server”，只要能握手即可（不用注册任何 service）
func startDummyGRPC(t *testing.T) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0") // 随机可用端口
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	s := grpc.NewServer()
	go func() { _ = s.Serve(lis) }()

	return lis.Addr().String(), func() {
		s.GracefulStop()
		_ = lis.Close()
	}
}

func putService(t *testing.T, cli *clientv3.Client, svcName, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	if _, err := cli.Put(ctx, key, addr); err != nil {
		t.Fatalf("etcd put %s: %v", key, err)
	}
}

func TestFetchAllServices_SkipSelfAndPopulateClientsAndRing(t *testing.T) {
	// 1) 准备：连 etcd（要求你本机已启动 etcd: localhost:2379）
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("create etcd client: %v", err)
	}
	defer etcdCli.Close()

	// 2) 准备：起两个“假 gRPC 节点”，当作远端节点 A/C
	addrA, stopA := startDummyGRPC(t)
	defer stopA()

	addrC, stopC := startDummyGRPC(t)
	defer stopC()

	// self 节点 B（不用真的起 server，因为 fetchAllServices 会跳过自己）
	selfAddr := "127.0.0.1:8002"

	// 3) 往 etcd 写入 3 条记录：A/B/C
	svcName := "go-cache"
	putService(t, etcdCli, svcName, addrA)
	putService(t, etcdCli, svcName, selfAddr)
	putService(t, etcdCli, svcName, addrC)

	// 4) 构造 picker（不走 NewClientPicker，避免它去用 DefaultConfig）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &ClientPicker{
		selfAddr: selfAddr,
		svcName:  svcName,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
		etcdCli:  etcdCli,
		ctx:      ctx,
		cancel:   cancel,
	}

	// 5) 调用：全量拉取
	if err := p.fetchAllServices(); err != nil {
		t.Fatalf("fetchAllServices: %v", err)
	}

	// 6) 断言：clients 里只有 A/C
	if len(p.clients) != 2 {
		t.Fatalf("clients len want=2 got=%d, clients=%v", len(p.clients), keysOfClientMap(p.clients))
	}
	if _, ok := p.clients[addrA]; !ok {
		t.Fatalf("missing client for addrA=%s", addrA)
	}
	if _, ok := p.clients[addrC]; !ok {
		t.Fatalf("missing client for addrC=%s", addrC)
	}
	if _, ok := p.clients[selfAddr]; ok {
		t.Fatalf("self should be skipped, but found selfAddr in clients: %s", selfAddr)
	}

	// 7) 断言：PickPeer(key) 不会选到 self
	for _, k := range []string{"k1", "k2", "k3", "user:100", "hot_key"} {
		peer, ok, self := p.PickPeer(k)
		if !ok {
			t.Fatalf("PickPeer(%q) should ok=true", k)
		}
		if self {
			t.Fatalf("PickPeer(%q) should not pick self", k)
		}
		if peer == nil {
			t.Fatalf("PickPeer(%q) returned nil peer", k)
		}
	}

	// 8) 清理（关闭 gRPC conn）
	_ = p.Close()
}

func keysOfClientMap(m map[string]*Client) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
