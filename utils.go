/***************************************************************
* 版权所有 (C)2025, Simon·Richard
* 完成时间: 2025.12.3 21:36
***************************************************************/
package gocache

import (
	"strings"
)

// addr = "127.0.0.1:8080" → t1 = []string{"127.0.0.1", "8080"}
// addr = "localhost:9000" → t1 = []string{"localhost", "9000"}
func ValidPeerAddr(addr string) bool {
	t1 := strings.Split(addr, ":")
	if len(t1) != 2 {
		return false
	}
	// TODO: more selections
	t2 := strings.Split(t1[0], ".")
	if t1[0] != "localhost" && len(t2) != 4 {
		return false
	}
	return true
}
