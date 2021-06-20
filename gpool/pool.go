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
func (p *pool) Submit(task interface{}) (err error) {
	// 接收到一个任务，此时应该怎么做？
	// 判断 pool 是否已经关闭
	if p.IsClosed() {
		return fmt.Errorf("pool is closed")
	}

	// 获取 worker 来执行任务
	var w *worker
	if w = p.getWorker(); w != nil {

	}
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

// setStatus
func (p *pool) setStatus(status int32) {
	atomic.StoreInt32(&p.status, status)
}

// Len
func (p *pool) Len() int32 {
	return atomic.LoadInt32(&p.len)
}

// Cap
func (p *pool) Cap() int32 {
	return atomic.LoadInt32(&p.cap)
}

// addWorker
func (p *pool) addWorker(w *worker) {
	p.workers.Put(w)
}

// getWorker
func (p *pool) getWorker() (w *worker) {
	if p.IsClosed() {
		return nil
	}
	// 获取 workder 的逻辑，比如目前的 worker 数是否已经超过了容量，获取成功了怎么做，获取失败了怎么做

	return w
}

// newLocker
func newLocker() sync.Locker {
	return &sync.Mutex{}
}
