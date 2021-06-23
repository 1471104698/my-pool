package main

import (
	"fmt"
	"gpool/gpool"
	"sync"
	"sync/atomic"
	"time"
)

var sum int32

func add(i int32) {
	atomic.AddInt32(&sum, i)
}

var wg sync.WaitGroup

func main() {
	wg = sync.WaitGroup{}

	var p = gpool.NewPool(1000, 1000, 2, gpool.WithIsBlocking(false),
		gpool.WithIsPreAllocation(true))
	times := 50000
	//times := 5000
	for i := 0; i < 1; i++ {
		p.Reboot()
		startTime := time.Now()
		sum = 0
		for i := 0; i < times; i++ {
			wg.Add(1)
			i := i
			p.Submit(func() {
				fmt.Println("run with：", i)
				add(int32(i))
				wg.Done()
			})
		}
		wg.Wait()
		fmt.Println("耗时：", time.Now().Sub(startTime))

		p.Close()
		fmt.Println("运行的线程数：", p.RunningSize())
		fmt.Println("最大线程数：", p.MaxSize())
		fmt.Println("预期 sum 值：", getSum(times))
		fmt.Println("实际 sum 值:", sum)
		if sum != getSum(times) {
			panic(fmt.Errorf("the final result is wrong!!!, expect:%v, actually:%v", sum, getSum(times)))
		}
	}
}

func getSum(j int) int32 {
	var sum int32
	for i := 0; i < j; i++ {
		sum += int32(i)
	}
	return sum
}
