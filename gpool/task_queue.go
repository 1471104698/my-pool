package gpool

import (
	"sync"
	"time"
)

const (
	// DefaultTaskQueueCap 默认任务队列容量大小
	DefaultTaskQueueCap = 1000
)

// taskQueue 任务队列，实现基本跟 WorkersQueue 一致，可惜目前没有泛型（虽然听说出了，不过还没用）
// 双向循环队列，头入队，尾出队，这里目前只支持一个 goroutine，如果要支持两个 goroutine，那么需要将 TaskFunc 进行封装，每个内置一把 lock
type taskQueue struct {
	// 容量
	cap int32
	// 元素个数
	len int32

	// 队首索引
	head int32
	// 队尾索引
	tail int32
	// 全局锁
	lock sync.Locker

	// 用于超时控制
	ch chan struct{}

	// 存储任务的容器
	tasks []TaskFunc
}

// NewTaskQueue 创建一个任务队列
func NewTaskQueue(cap int32) *taskQueue {
	if cap <= 0 {
		cap = DefaultTaskQueueCap
	}
	return &taskQueue{
		cap:   cap,
		len:   0,
		head:  0,
		tail:  0,
		lock:  newLocker(),
		ch:    make(chan struct{}),
		tasks: make([]TaskFunc, cap, cap),
	}
}

// Add 添加，成功返回 true，失败返回 false，不阻塞等待
func (q *taskQueue) Add(task TaskFunc) bool {
	if task == nil {
		return false
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.enqueue(task)
}

// Poll 获取，成功返回 任务，失败返回 nil，不阻塞等待
func (q *taskQueue) Poll() (task TaskFunc) {
	q.lock.Lock()
	defer q.lock.Unlock()

	task = q.dequeue()
	return task
}

// PollWithTimeout 超时获取，没有数据时会阻塞等待一段时间
func (q *taskQueue) PollWithTimeout(timeout int32, duration time.Duration) (task TaskFunc) {
	endTime := time.Now().Add(time.Duration(timeout) * duration)
	for {
		if task = q.Poll(); task != nil {
			return task
		}
		remaining := endTime.Sub(time.Now())
		if remaining < 0 {
			break
		}
		// 等待添加时传入的信息
		select {
		case <-q.ch:
		case <-time.After(remaining):
		}
	}
	return nil
}

// IsFull 判断队列是否已满
func (q *taskQueue) IsFull() bool {
	return q.len == q.cap
}

// IsEmpty 判断队列是否为空
func (q *taskQueue) IsEmpty() bool {
	return q.len == 0
}

// enqueue 入队，调用该方法时必须获取锁
func (q *taskQueue) enqueue(task TaskFunc) bool {
	if q.IsFull() {
		return false
	}
	q.tasks[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.len++
	// 添加的时候传递信息，用于 PollWithTimeout，这里并不进行无限期的等待，超时等待
	select {
	case q.ch <- struct{}{}:
	case <-time.After(time.Nanosecond):
	}
	return true
}

// deque 出队，调用该方法时必须获取锁
func (q *taskQueue) dequeue() (task TaskFunc) {
	if q.IsEmpty() {
		return nil
	}
	task = q.tasks[q.head]
	q.tasks[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.len--
	return task
}

// reset 重置 任务队列，清空所有的任务，初始化状态
func (q *taskQueue) reset() {
	q.lock.Lock()
	defer q.lock.Unlock()
	for i := 0; i < int(q.len); i++ {
		q.tasks[i] = nil
	}
	q.len = 0
	q.head = 0
	q.tail = 0
}
