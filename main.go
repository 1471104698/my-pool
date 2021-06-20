package main

import (
	"fmt"
	"gpool/gpool"
	"sync/atomic"
	"time"
)

var sum int32

func add(i int32) {
	atomic.AddInt32(&sum, i)
}

var errs int32

func addErr(i int32) {
	atomic.AddInt32(&errs, i)
}

var p = gpool.NewPool(5, 20, 2)

func main() {

	times := 2000
	test(times)
}

func test(times int) {
	for i := 0; i < times; i++ {
		i := i
		err := p.Submit(func() {
			add(int32(i))
		})
		if err != nil {
			addErr(1)
		}
	}

	time.Sleep(2 * time.Second)
	p.Close()
	fmt.Println("运行的线程数：", p.RunningSize())
	fmt.Println("最大线程数：", p.MaxSize())
	time.Sleep(2 * time.Second)
	fmt.Println("运行的线程数：", p.RunningSize())
	fmt.Println("最大线程数：", p.MaxSize())

	fmt.Println("预期 sum 值：", getSum(times))
	fmt.Println("实际 sum 值:", sum)
	fmt.Println("发生错误数 errs:", errs)
	if sum != getSum(times) {
		panic("the final result is wrong!!!")
	}
}

func getSum(j int) int32 {
	var sum int32
	for i := 0; i < j; i++ {
		sum += int32(i)
	}
	return sum
}
