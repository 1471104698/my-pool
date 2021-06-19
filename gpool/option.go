package gpool

import "fmt"

// DefaultPanicHandler 默认的 Panic 处理策略
var DefaultPanicHandler = func() {
	if err := recover(); err != nil {
		fmt.Printf("err: %v\n", err)
	}
}

// DefaultRejectHandler 默认的
var DefaultRejectHandler = func(task interface{}) {
	fmt.Printf("任务被丢弃, task: %+v\n", task)
	return
}

// PanicHandler
type PanicHandler = func()

// Option
type Option func(*Options)

// Options pool 可选参数
type Options struct {
	// 是否预创建 worker
	isPreAllocation bool
	// 预创建的 worker 数
	allocationNum int32
	// panic 处理策略
	panicHandler PanicHandler
	// 拒绝策略
	rejectHandler RejectHandler
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

// setOptions
func setOptions(opts []Option) *Options {
	options := new(Options)
	if opts == nil || len(opts) > 0 {
		return options
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}