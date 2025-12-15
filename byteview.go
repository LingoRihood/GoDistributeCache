/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.3 21:36
* 把缓存里的数据用“只读视图”封装起来，避免外部拿到内部的 []byte 后随意修改，破坏缓存一致性
***************************************************************/

package gocache

// ByteView 只读的字节视图，用于缓存数据
type ByteView struct {
	// type byte = uint8
	b []byte
}

// 返回数据长度
func (b ByteView) Len() int {
	return len(b.b)
}

// 拷贝一份字节切片，返回一个新的切片，跟原来的底层数组不共享内存。
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// 对外返回字节切片（拷贝版）
func (b ByteView) ByteSLice() []byte {
	return cloneBytes(b.b)
}

// 把字节视图转成字符串
// 从 []byte 转 string 通常会发生一次拷贝（确保字符串不可变）
func (b ByteView) String() string {
	return string(b.b)
}
