package gpool

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	fullErr  = fmt.Errorf("queue is full")
	emptyErr = fmt.Errorf("queue is empty")
)

const (
	// defaultWorkersCap
	defaultWorkersCap = 100
)

// workers
type workers struct {
	cap int32
	len int32

	lock     sync.Locker
	producer *sync.Cond
	consumer *sync.Cond

	workers []*worker
}

// NewWorkers
func NewWorkers(cap int32) (ws *workers) {
	if cap <= 0 {
		cap = defaultWorkersCap
	}
	lock := newLocker()
	return &workers{
		cap:  cap,
		len:  0,
		lock: lock,
		// producer 和 consumer 同一把锁
		producer: sync.NewCond(lock),
		consumer: sync.NewCond(lock),
		workers:  make([]*worker, 0, cap),
	}
}

// Add 添加，满了返回错误
func (ws *workers) Add(w *worker) error {
	if ws.Offer(w) {
		return fullErr
	}
	return nil
}

// Remove 移除，空的返回错误
func (ws *workers) Remove() (w *worker, err error) {
	if w = ws.Poll(); w == nil {
		return nil, emptyErr
	}
	return w, nil
}

// Offer 添加，满了返回 false
func (ws *workers) Offer(w *worker) bool {
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
func (ws *workers) Poll() (w *worker) {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	if ws.IsEmpty() {
		return nil
	}
	return ws.dequeue()
}

// Put 添加，满了阻塞等待
func (ws *workers) Put(w *worker) {
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
func (ws *workers) Take() (w *worker) {
	ws.lock.Lock()
	defer ws.lock.Unlock()

	for ws.IsEmpty() {
		ws.consumer.Wait()
	}

	return ws.dequeue()
}

// enqueue 将 w 入队，调用该方法的都是已经获取锁的
func (ws *workers) enqueue(w *worker) {
	ws.workers = append(ws.workers, w)
	ws.len++
	// 唤醒一个消费者消费
	ws.consumer.Signal()
}

// dequeue 将 w 出队，调用该方法的都是已经获取锁的
func (ws *workers) dequeue() (w *worker) {
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

// checkWorker
func (ws *workers) checkWorker(i int32) {
	ws.lock.Lock()
	defer ws.lock.Unlock()
	if i >= ws.len || i < 0 {
		return
	}
	if ws.workers[i].IsStop() {
		ws.workers[i] = nil
		ws.workers = append(ws.workers[0:i], ws.workers[i+1:]...)
	}
}

// IsFull
func (ws *workers) IsFull() bool {
	return atomic.LoadInt32(&ws.len) == ws.cap
}

// IsEmpty
func (ws *workers) IsEmpty() bool {
	return atomic.LoadInt32(&ws.len) == 0
}

// reset
func (ws *workers) reset() {
	for k, w := range ws.workers {
		w.setStatus(WorkerStop)
		ws.workers[k] = nil
	}
	ws.len = 0
	ws.producer.Broadcast()
	ws.consumer.Broadcast()
	ws.workers = ws.workers[:0]
}
