package gpool

import (
	"sync"
)

const (
	// defaultTaskQueueCap
	defaultTaskQueueCap = 100
)

// taskFunc
type taskFunc = func()

// taskQueue 任务队列，实现基本跟 workers 一致，可惜目前没有泛型（虽然听说出了，不过还没用）
type taskQueue struct {
	cap int32
	len int32

	lock sync.Locker

	tasks []*taskFunc
}

func NewTaskQueue(cap int32) *taskQueue {
	if cap <= 0 {
		cap = defaultTaskQueueCap
	}
	lock := newLocker()
	return &taskQueue{
		cap:   cap,
		len:   0,
		lock:  lock,
		tasks: make([]*taskFunc, 0, cap),
	}
}
