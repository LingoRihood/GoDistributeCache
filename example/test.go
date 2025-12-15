/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.12 16:47
***************************************************************/

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	lcache "github.com/LingoRihood/GoDistributeCache"
)

func main() {
	// 添加命令行参数，用于区分不同节点
	// 定义一个命令行参数：-port
	// 不带参数：go run main.go → 端口 = 8001
	// 指定端口：go run main.go -port=8002 → 端口 = 8002
	// 返回的是 *int（指针），所以 port 是个“指向整数的指针”
	port := flag.Int("port", 8001, "节点端口")

	// 返回的是 *string，所以 nodeID 是个“指向字符串的指针”
	nodeID := flag.String("node", "A", "节点标识符")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("[节点%s] 启动，地址: %s", *nodeID, addr)

	// 创建节点
	// 一个“自己本机的缓存节点对象”(node)
	// 启动一个缓存 Server（注册到 etcd，提供 gRPC 服务）
	// addr：这是这个缓存节点自己对外提供 gRPC 服务的监听地址(我这个节点对外提供服务的端口是 8001，别人要找我干活就连我 8001)
	node, err := lcache.NewServer(addr, "go-cache",
		// 电话簿(通讯录)在本机的 2379 端口，去那里登记/查人
		lcache.WithEtcdEndpoints([]string{"localhost:2379"}),
		lcache.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal("创建节点失败:", err)
	}

	// 创建节点选择器
	// 创建一个 ClientPicker（从 etcd 发现其他节点 + 一致性哈希选节点）
	picker, err := lcache.NewClientPicker(addr)
	if err != nil {
		log.Fatal("创建节点选择器失败:", err)
	}

	// 创建缓存组 Group：管理本地缓存 + 统一读写逻辑（本地 / 远程 / 回源）
	// GetterFunc(这个函数) 把它“转成 GetterFunc 类型”
	group := lcache.NewGroup("test", 2<<20, lcache.GetterFunc(
		func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[节点%s] 触发数据源加载: key=%s", *nodeID, key)

			// []byte(fmt.Sprintf(...)) 会先生成一个字符串，再拷贝一份到 []byte，
			// 可以用 fmt.Appendf 直接往 []byte 里写，少一次内存分配
			// return []byte(fmt.Sprintf("节点%s的数据源值", *nodeID)), nil
			buf := make([]byte, 0, 32) // 预估一下长度，不重要
			buf = fmt.Appendf(buf, "节点%s的数据源值", *nodeID)
			return buf, nil
		}),
	)

	// 注册节点选择器(把 picker 注册给 group：让 group 具备“分布式能力”)
	group.RegisterPeers(picker)

	// 启动 Server（异步 goroutine）并把 ctx 作为生命周期控制
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	// 启动节点(放到 goroutine 里：因为 Start 会阻塞（Serve 一直跑）)
	go func() {
		log.Printf("[节点%s] 开始启动服务...", *nodeID)
		if err := node.Start(ctx1); err != nil {
			log.Fatal("启动节点失败:", err)
		}
	}()

	// 等待节点注册完成
	log.Printf("[节点%s] 等待节点注册...", *nodeID)
	time.Sleep(5 * time.Second)

	ctx := context.Background()

	// 往 group 写入一个“本节点专属 key”, 设置本节点的特定键值对
	localKey := fmt.Sprintf("key_%s", *nodeID)
	// localValue := []byte(fmt.Sprintf("这是节点%s的数据", *nodeID))

	buf := make([]byte, 0, 32) // 预估一个容量，随便写
	buf = fmt.Appendf(buf, "这是节点%s的数据", *nodeID)
	localValue := buf

	fmt.Printf("\n=== 节点%s：设置本地数据 ===\n", *nodeID)
	err = group.Set(ctx, localKey, localValue)
	if err != nil {
		log.Fatal("设置本地数据失败:", err)
	}
	fmt.Printf("节点%s: 设置键 %s 成功\n", *nodeID, localKey)

	// 等待其他节点也完成设置
	log.Printf("[节点%s] 等待其他节点准备就绪...", *nodeID)
	time.Sleep(30 * time.Second)

	// 打印当前已发现的节点
	picker.PrintPeers()

	// 测试获取本地数据
	fmt.Printf("\n=== 节点%s：获取本地数据 ===\n", *nodeID)
	fmt.Printf("直接查询本地缓存...\n")

	// 打印缓存统计信息
	stats := group.Stats()
	fmt.Printf("缓存统计: %+v\n", stats)

	if val, err := group.Get(ctx, localKey); err == nil {
		fmt.Printf("节点%s: 获取本地键 %s 成功: %s\n", *nodeID, localKey, val.String())
	} else {
		fmt.Printf("节点%s: 获取本地键失败: %v\n", *nodeID, err)
	}

	// 测试获取其他节点的数据
	otherKeys := []string{"key_A", "key_B", "key_C"}
	for _, key := range otherKeys {
		if key == localKey {
			continue // 跳过本节点的键
		}
		fmt.Printf("\n=== 节点%s：尝试获取远程数据 %s ===\n", *nodeID, key)
		log.Printf("[节点%s] 开始查找键 %s 的远程节点", *nodeID, key)
		if val, err := group.Get(ctx, key); err == nil {
			fmt.Printf("节点%s: 获取远程键 %s 成功: %s\n", *nodeID, key, val.String())
		} else {
			fmt.Printf("节点%s: 获取远程键失败: %v\n", *nodeID, err)
		}
	}

	// 主 goroutine 一直卡在这里(select {} 是 Go 里最简单的“永久阻塞”的写法)
	// for {}（但 for 会空转吃 CPU）
	select {}
}
