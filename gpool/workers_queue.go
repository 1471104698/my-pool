package gpool

import (
	"sync"
	"sync/atomic"
)

// WorkersQueue
type WorkersQueue struct {
	// 容量
	cap int32
	// 元素个数
	len int32

	// 全局锁
	lock sync.Locker
	// 生产者
	producer *sync.Cond
	// 消费者
	consumer *sync.Cond

	// worker 容器
	workers []Worker
}

// NewWorkersQueue 创建一个 WorkersQueue
func NewWorkersQueue(cap int32) (ws *WorkersQueue) {
	if cap <= 0 {
		cap = DefaultWorkerCap
	}
	lock := newLocker()
	return &WorkersQueue{
		cap:  cap,
		len:  0,
		lock: lock,
		// producer 和 consumer 同一把锁
		producer: sync.NewCond(lock),
		consumer: sync.NewCond(lock),
		workers:  make([]Worker, 0),
	}
}

// Add 添加，满了返回错误
func (ws *WorkersQueue) Add(w Worker) error {
	if ws.Offer(w) {
		return fullErr
	}
	return nil
}

// Remove 移除，空的返回错误
func (ws *WorkersQueue) Remove() (w Worker, err error) {
	if w = ws.Poll(); w == nil {
		return nil, emptyErr
	}
	return w, nil
}

// Offer 添加，满了返回 false
func (ws *WorkersQueue) Offer(w Worker) bool {
	if w == nil {
		return true
	}
	// 保证并发安全，因为同一时间可能存在多个 goroutine 来操作
	ws.lock.Lock()
	defer ws.lock.Unlock()

	if ws.IsFull() {
		return false
	}
	ws.enqueue(w)
	return true
}

// Poll 移除，空的返回 nil
func (ws *WorkersQueue) Poll() (w Worker) {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	if ws.IsEmpty() {
		return nil
	}
	return ws.dequeue()
}

// Put 添加，满了阻塞等待
func (ws *WorkersQueue) Put(w Worker) {
	if w == nil {
		return
	}
	ws.lock.Lock()
	defer ws.lock.Unlock()

	for ws.IsFull() {
		ws.producer.Wait()
	}

	ws.enqueue(w)
}

// Take 移除，空的阻塞等待
func (ws *WorkersQueue) Take() (w Worker) {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	for ws.IsEmpty() {
		ws.consumer.Wait()
	}

	return ws.dequeue()
}

// IsFull 判断队列是否已满
func (ws *WorkersQueue) IsFull() bool {
	return ws.Len() == ws.cap
}

// IsEmpty 判断队列是否为空
func (ws *WorkersQueue) IsEmpty() bool {
	return ws.Len() == 0
}

// Len 获取元素个数
func (ws *WorkersQueue) Len() int32 {
	return atomic.LoadInt32(&ws.len)
}

// Cap 获取容量
func (ws *WorkersQueue) Cap() int32 {
	return ws.cap
}

// enqueue 将 w 入队，调用该方法的都是已经获取锁的
func (ws *WorkersQueue) enqueue(w Worker) {
	ws.workers = append(ws.workers, w)
	ws.len++
	// 唤醒一个消费者消费
	ws.consumer.Signal()
}

// dequeue 将 w 出队，调用该方法的都是已经获取锁的
func (ws *WorkersQueue) dequeue() (w Worker) {
	l := ws.len
	w = ws.workers[l-1]
	// 帮助 GC 回收
	ws.workers[l-1] = nil
	ws.workers = ws.workers[:l-1]
	ws.len--
	// 唤醒一个生产者生产
	ws.producer.Signal()
	return w
}

// checkWorker 检查 worker 是否正在运行，如果已经停止运行，那么将它移除
func (ws *WorkersQueue) checkWorker(i int32) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	if i >= ws.len || i < 0 {
		return
	}
	if ws.workers[i].IsStop() {
		ws.workers[i] = nil
		ws.workers = append(ws.workers[0:i], ws.workers[i+1:]...)
		ws.len--
	}
}

// reset 重置 WorkersQueue 队列，清空所有的 worker，初始化状态
func (ws *WorkersQueue) reset() {
	for k, w := range ws.workers {
		// 通知退出
		w.notifyExit()
		ws.workers[k] = nil
	}
	ws.len = 0
	ws.producer.Broadcast()
	ws.consumer.Broadcast()
	ws.workers = ws.workers[:0]
}
