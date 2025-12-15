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
func (g *Group) Do(key string, fn func() (any, error)) (any, error) {
	// 先看有没有“正在进行中的请求”
	// Check if there is already an ongoing call for this key
	// sync.Map 的 key、value 类型都是 interface{}，使用时要做类型断言
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)

		// 当前 goroutine 不再执行 fn，而是等待已经在执行的那次调用结束。
		// 等待期间，这个 goroutine 会阻塞在这里。
		c.wg.Wait()         // Wait for the existing request to finish
		return c.val, c.err // Return the result from the ongoing call
	}

	// If no ongoing request, create a new one
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c) // Store the call in the map

	// Execute the function and set the result
	c.val, c.err = fn()
	c.wg.Done() // Mark the request as done

	// After the request is done, clean up the map
	g.m.Delete(key)

	return c.val, c.err
}
