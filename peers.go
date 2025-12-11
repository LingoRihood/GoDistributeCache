package gocache

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"github.com/sirupsen/logrus"
	"github.com/LingoRihood/GoDistributeCache/consistenthash"
	"github.com/LingoRihood/GoDistributeCache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "go-cache"

// PeerPicker 定义了peer选择器的接口
type PeerPicker interface {
	PickPeer(key string) (peer Peer, ok bool, self bool)
	Close() error
}

// Peer 定义了缓存节点的接口
type Peer interface {
	Get(group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(group string, key string) (bool, error)
	Close() error
}