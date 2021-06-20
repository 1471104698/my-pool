package gpool

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	// pool 默认容量
	DefaultCap = 16
	// pool 最大容量
	MaxCap = 1 << 10
)

// pool 的状态
const (
	// 初始状态，目前还没有线程在执行
	Init = iota
	// pool 正在运行
	Running
	// pool 已经关闭
	Closed
)

var (
	poolClosedErr = fmt.Errorf("pool is closed")
	poolFullErr   = fmt.Errorf("pool is full")
)

// pool
type pool struct {
	cap    int32
	len    int32
	free   int32
	status int32

	cond *sync.Cond

	lock sync.Locker

	workers *workers // 这里实际上应该将 workers 做成一个接口，这样可以接收不同实现的任务队列

	opts *Options
}

// NewPool
func NewPool(cap int32, opts ...Option) *pool {
	p := &pool{
		cap:     cap,
		len:     0,
		status:  Init,
		lock:    newLocker(),
		cond:    sync.NewCond(newLocker()),
		opts:    setOptions(opts),
		workers: NewWorkers(cap),
	}
	p.init()
	return p
}

// init
func (p *pool) init() {
	if p.cap < 0 {
		p.cap = DefaultCap
	}
	if p.cap > MaxCap {
		p.cap = MaxCap
	}
	if p.opts.rejectHandler == nil {
		p.opts.rejectHandler = defaultRejectHandler
	}
	if p.opts.panicHandler == nil {
		p.opts.panicHandler = defaultPanicHandler
	}
	if p.opts.logger == nil {
		p.opts.logger = defaultLogger
	}
}

// Submit
func (p *pool) Submit(task func()) error {
	// 接收到一个任务，此时应该怎么做？
	// 判断 pool 是否已经关闭
	if p.IsClosed() {
		return poolClosedErr
	}

	// 获取 worker 来执行任务
	var w *worker
	if w = p.getWorker(); w == nil {
		return poolFullErr
	}
	w.task <- task
	return nil
}

// IsRunning
func (p *pool) IsRunning() bool {
	return atomic.LoadInt32(&p.status) == Running
}

// IsRunning
func (p *pool) IsClosed() bool {
	// atomic.LoadInt32() 原子性的获取某个值
	return atomic.LoadInt32(&p.status) == Closed
}

// IsFull
func (p *pool) IsFull() bool {
	return p.Cap() == p.Len()
}

// Len
func (p *pool) Len() int32 {
	return atomic.LoadInt32(&p.len)
}

// Cap
func (p *pool) Cap() int32 {
	return atomic.LoadInt32(&p.cap)
}

// ---------------------------------------------------------------------------------------------------

// newLocker
func newLocker() sync.Locker {
	return &sync.Mutex{}
}

// incrLen
func (p *pool) incrLen(i int32) {
	atomic.AddInt32(&p.len, i)
}

// setStatus
func (p *pool) setStatus(status int32) {
	atomic.StoreInt32(&p.status, status)
}

// addWorker
func (p *pool) addWorker(w *worker) {
	p.workers.Put(w)
}

// getWorker
// 获取 workder 的逻辑，比如目前的 worker 数是否已经超过了容量，获取成功了怎么做，获取失败了怎么做
func (p *pool) getWorker() (w *worker) {
	if p.IsClosed() {
		return nil
	}
	// 从 workers 中获取一个可用的 worker
	// 这里由 workers 自己保证并发安全
	w, _ = p.workers.Remove()
	if w != nil {
		return w
	}
	// 无需加锁，利用 atomic 实现原子性即可

	// 判断已经存在的 worker 数是否已经到达 cap
	if p.IsFull() {
		return nil
	}
	// 创建一个新的 worker
	w = NewWorker(p)
	// 让 worker 先开始运行等待任务
	w.run()

	// 这里不能将 worker 入队，因为 workers 内部的 worker 存储的是空闲等待任务的，而这里新创建的是需要去执行任务的
	return w
}
