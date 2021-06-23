package gpool

import (
	"fmt"
	"time"
)

// RejectHandler
type RejectHandler = func(task func()) (err error)

// PanicHandler
type PanicHandler = func(err interface{})

var (
	// 默认的 Panic 处理策略
	defaultPanicHandler = func(err interface{}) {
		fmt.Printf("发生 panic err: %v\n", err)
	}
	// 默认的拒绝策略
	defaultRejectHandler = func(task func()) (err error) {
		//fmt.Printf("任务被丢弃, tast: %#v\n", task)
		return fmt.Errorf("任务被丢弃")
	}
)

// Option
type Option func(*Options)

// Options pool 可选参数
type Options struct {
	// workers 清理周期
	cleanTime time.Duration
	// 是否预创建 worker
	isPreAllocation bool
	// 预创建的 worker 数
	allocationNum int32
	// panic 处理策略
	panicHandler PanicHandler
	// 拒绝策略
	rejectHandler RejectHandler
	// 当任务来临而没有 worker 可以创建，同时任务队列已满的时候是否阻塞当前 goroutine 等待出现空闲的 worker
	isBlocking bool
	// 最大的阻塞 goroutine 数
	blockMaxNum int32
	// 阻塞超时时间
	blockingTime time.Duration
}

// WithCleanTime
func WithCleanTime(cleanTime time.Duration) Option {
	return func(opt *Options) {
		opt.cleanTime = cleanTime
	}
}

// WithIsPreAllocation
func WithIsPreAllocation(isPreAllocation bool) Option {
	return func(opt *Options) {
		opt.isPreAllocation = isPreAllocation
	}
}

// WithAllocationNum
func WithAllocationNum(allocationNum int32) Option {
	return func(opt *Options) {
		opt.allocationNum = allocationNum
	}
}

// WithPanicHandler
func WithPanicHandler(panicHandler PanicHandler) Option {
	return func(opt *Options) {
		opt.panicHandler = panicHandler
	}
}

// WithRejectHandler
func WithRejectHandler(rejectHandler RejectHandler) Option {
	return func(opt *Options) {
		opt.rejectHandler = rejectHandler
	}
}

// WithIsBlocking
func WithIsBlocking(isBlocking bool) Option {
	return func(opt *Options) {
		opt.isBlocking = isBlocking
	}
}

// WithBlockMaxNum
func WithBlockMaxNum(blockMaxNum int32) Option {
	return func(opt *Options) {
		opt.blockMaxNum = blockMaxNum
	}
}

// WithBlockingTime
func WithBlockingTime(blockingTime time.Duration) Option {
	return func(opt *Options) {
		opt.blockingTime = blockingTime
	}
}

// setOptions
func setOptions(opts []Option) *Options {
	options := new(Options)
	if opts == nil || len(opts) == 0 {
		return options
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
