package gpool

import (
	"fmt"
	"math"
)

// 常用错误
var (
	fullErr  = fmt.Errorf("queue is full")
	emptyErr = fmt.Errorf("queue is empty")
)

// 默认 workers 容量
const DefaultWorkerCap = math.MaxInt32

// workers 类型
const (
	WorkerQueue = iota
)

// Interface 通用容器接口
type Interface interface {
	Add(w Worker) error            // Add 添加，满了返回错误
	Remove() (w Worker, err error) // Remove 移除，空的返回错误
	Offer(w Worker) bool           // Offer 添加，满了返回 false
	Poll() (w Worker)              // Poll 移除，空的返回 nil
	Put(w Worker)                  // Poll 移除，空的返回 nil
	Take() (w Worker)              // Take 移除，空的阻塞等待
	IsFull() bool                  // IsFull 判断队列是否已满
	IsEmpty() bool                 // IsEmpty 判断队列是否为空
	Len() int32                    // Len 获取元素个数
	Cap() int32                    // Cap 获取容量
}

// PoolWorkers goroutine pool(gpool) workers 接口
type PoolWorkers interface {
	Interface
	// checkWorker 检查某个 worker 状态，内部使用时才进行调用
	checkWorker(i int32)
	// reset 重置 workers 状态，内部使用时才进行调用
	reset()
}

// NewPoolWorkers 获取一个 Pool workers
func NewPoolWorkers(tape, cap int32) PoolWorkers {
	switch tape {
	case WorkerQueue:
		return NewWorkersQueue(cap)
	default:
		return NewWorkersQueue(cap)
	}
}
