package gpool

import (
	"sync"
)

const (
	// pool 默认容量
	DefaultCap = 10
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

type RejectHandler = func(task interface{})

// pool
type pool struct {
	cap int32
	len int32
	status int32

	cond *sync.Cond

	workers *workers

	opts *Options


}

// NewPool
func NewPool(cap int32, opts ...Option) *pool {

	p := &pool{
		cap: cap,
		len: 0,
		status: Init,
		cond: sync.NewCond(&sync.Mutex{}),
		opts: setOptions(opts),
	}
	p.setInit()
	return p
}

// setInit
func (p *pool) setInit() {
	if p.cap < 0 {
		p.cap = DefaultCap
	}
	if p.opts.rejectHandler == nil {
		p.opts.rejectHandler = DefaultRejectHandler
	}
	if p.opts.panicHandler == nil {
		p.opts.panicHandler = DefaultPanicHandler
	}
}


// Submit
func (p *pool) Submit(task interface{}) (err error) {
	return nil
}

