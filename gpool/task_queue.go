package gpool

import (
	"sync"
	"time"
)

const (
	// DefaultTaskQueueCap
	DefaultTaskQueueCap = 10000
)

// taskFunc
type taskFunc = func()

// taskQueue 任务队列，实现基本跟 workers 一致，可惜目前没有泛型（虽然听说出了，不过还没用）
// 双向循环队列，头入队，尾出队，这里目前只支持一个 goroutine，如果要支持两个 goroutine，那么需要将 taskFunc 进行封装，每个内置一把 lock
type taskQueue struct {
	cap int32
	len int32

	head int32
	tail int32
	lock sync.Locker

	ch chan struct{}

	tasks []taskFunc
}

// NewTaskQueue
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
		tasks: make([]taskFunc, cap, cap),
	}
}

// Add
func (q *taskQueue) Add(task taskFunc) bool {
	if task == nil {
		return false
	}
	q.lock.Lock()
	defer q.lock.Unlock()

	return q.enqueue(task)
}

// Poll
func (q *taskQueue) Poll() (task taskFunc) {
	q.lock.Lock()
	defer q.lock.Unlock()

	task = q.dequeue()
	return task
}

// PollWithTimeout
func (q *taskQueue) PollWithTimeout(timeout int32, duration time.Duration) (task taskFunc) {
	endTime := time.Now().Add(time.Duration(timeout) * duration)
	for {
		if task = q.Poll(); task != nil {
			return task
		}
		remaining := endTime.Sub(time.Now())
		if remaining < 0 {
			break
		}
		select {
		case <-q.ch:
		case <-time.After(remaining):
		}
	}
	return nil
}

// enqueue 入队，调用该方法时必须获取锁
func (q *taskQueue) enqueue(task taskFunc) bool {
	if q.isFull() {
		return false
	}
	q.tasks[q.tail] = task
	q.tail = (q.tail + 1) % q.cap
	q.len++
	select {
	case q.ch <- struct{}{}:
	case <-time.After(time.Millisecond):
	}
	return true
}

// deque
func (q *taskQueue) dequeue() (task taskFunc) {
	if q.isEmpty() {
		return nil
	}
	task = q.tasks[q.head]
	q.tasks[q.head] = nil
	q.head = (q.head + 1) % q.cap
	q.len--
	return task
}

// isFull
func (q *taskQueue) isFull() bool {
	return q.len == q.cap
}

// isEmpty
func (q *taskQueue) isEmpty() bool {
	return q.len == 0
}

// reset
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
