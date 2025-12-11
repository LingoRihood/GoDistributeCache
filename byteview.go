/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.3 21:36
***************************************************************/

package gocache

// ByteView 只读的字节视图，用于缓存数据
type ByteView struct {
	// type byte = uint8
	b []byte
}

func (b ByteView) Len() int {
	return len(b.b)
}

// 拷贝一份字节切片，返回一个新的切片，跟原来的底层数组不共享内存。
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func (b ByteView) ByteSLice() []byte {
	return cloneBytes(b.b)
}

func (b ByteView) String() string {
	return string(b.b)
}