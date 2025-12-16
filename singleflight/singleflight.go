/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.3 21:36
***************************************************************/

package singleflight

import (
	"sync"
)

// 代表正在进行或已结束的请求
type call struct {
	// 用来让其它 goroutine 等待这次调用结束
	wg sync.WaitGroup
	// 调用的返回值
	val any
	// 调用返回的错误信息
	err error
}

// Group 结构体：管理所有 key 对应的调用
// Group manages all kinds of calls
type Group struct {
	m sync.Map // 使用sync.Map来优化并发性能
}

// 核心方法 Do：同 key 的请求合并执行
// Do 针对相同的key，保证多次调用Do()，都只会调用一次fn
// func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
// func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
// 	// 先看有没有“正在进行中的请求”
// 	// Check if there is already an ongoing call for this key
// 	// sync.Map 的 key、value 类型都是 interface{}，使用时要做类型断言
// 	if existing, ok := g.m.Load(key); ok {
// 		c := existing.(*call)

// 		// 当前 goroutine 不再执行 fn，而是等待已经在执行的那次调用结束。
// 		// 等待期间，这个 goroutine 会阻塞在这里。
// 		c.wg.Wait()         // Wait for the existing request to finish
// 		return c.val, c.err // Return the result from the ongoing call
// 	}

// 	// If no ongoing request, create a new one
// 	c := &call{}
// 	c.wg.Add(1)
// 	g.m.Store(key, c) // Store the call in the map

// 	// Execute the function and set the result
// 	c.val, c.err = fn()
// 	c.wg.Done() // Mark the request as done

// 	// After the request is done, clean up the map
// 	g.m.Delete(key)

// 	return c.val, c.err
// }

/*
如果你写成“先 Load，再 Store”，会有竞态：
G1 Load 没有
G2 Load 也没有
G1 Store
G2 也 Store（覆盖/重复）
两个人都执行 fn → singleflight 失效
*/

// func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
// 	c := &call{}
// 	c.wg.Add(1)

// 	// 如果 map 里 已经有 key：不会覆盖, 返回已有值到 actual, loaded = true
// 	// 如果 map 里 没有 key：把你传进去的 c 存进去, actual == c, loaded = false
// 	// 一句话：谁先抢到 Store 权，谁就是“第一个请求”
// 	// LoadOrStore 是原子的，能保证不会出现“两个人都以为自己是第一个”
// 	actual, loaded := g.m.LoadOrStore(key, c)
// 	if loaded {
// 		// 已有人在执行
// 		c2 := actual.(*call)
// 		c2.wg.Wait()
// 		return c2.val, c2.err
// 	}

// 	// 我是第一个，负责执行
// 	c.val, c.err = fn()
// 	c.wg.Done()

// 	// 	把这个 key 的“正在执行标记”删掉
// 	// 	→ 下次再来同一个 key，就会重新创建新的 call 再执行一次
// 	g.m.Delete(key)
// 	return c.val, c.err
// }

func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
	// 先准备一个“占位 call”，可能会被存进去，也可能不用
	c := &call{}
	c.wg.Add(1)

	// 原子操作：要么我把 c 放进去成为“第一个”，要么拿到别人放进去的 call
	actual, loaded := g.m.LoadOrStore(key, c)
	if loaded {
		// 已经有人在跑这个 key 了，我等它跑完，直接复用它的结果
		c2 := actual.(*call)
		c2.wg.Wait()
		return c2.val, c2.err
	}

	// 走到这里说明：我是第一个（actual==c），我负责执行 fn
	// 关键：用 defer 保证不管 fn 正常返回/报错/甚至 panic，都能 Done + Delete，避免别人永远卡住
	defer func() {
		// 先唤醒所有等待者
		c.wg.Done()
		// 再清理 key，允许下次请求重新触发 fn
		g.m.Delete(key)

		// 如果 fn panic，把 panic 继续抛出去（也可以选择转成 error）
		// recover() 是 Go 提供的“抓 panic 的网”
		/*
			如果 fn() 里发生 panic（比如数组越界、nil 指针），那 Done() 永远执行不到：
			所有 Wait() 的 goroutine 会永久阻塞
			key 也不会被 Delete，后续请求还会一直 Wait（相当于“死锁”）
		*/
		if r := recover(); r != nil {
			panic(r)
		}
	}()

	c.val, c.err = fn()
	return c.val, c.err
}
