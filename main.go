package main

import (
	"fmt"
	"gpool/gpool"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

func add(sum *int32, i int32) {
	time.Sleep(100 * time.Millisecond)
	atomic.AddInt32(sum, i)
}

func main() {
	var sum int32
	wg := sync.WaitGroup{}

	go seeStack()

	var p = gpool.NewPool(1000, 1000, 2, gpool.WithIsBlocking(false),
		gpool.WithIsPreAllocation(true))
	times := 3000
	//times := 50000
	for i := 0; i < 1; i++ {
		p.Reboot()
		startTime := time.Now()
		sum = 0
		for i := 0; i < times; i++ {
			wg.Add(1)
			i := i
			p.Submit(func() {
				fmt.Println(i)
				add(&sum, int32(i))
				wg.Done()
			})
		}
		wg.Wait()
		fmt.Println("耗时：", time.Now().Sub(startTime))
		p.Close()
		except := getSum(times)
		fmt.Println("预期 sum 值：", except)
		fmt.Println("实际 sum 值:", sum)
		if sum != except {
			panic(fmt.Errorf("expect:%v, actually:%v", sum, except))
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

const (
	pprofAddr string = ":7890"
)

func seeStack() {
	pprofHandler := http.NewServeMux()
	pprofHandler.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	server := &http.Server{Addr: pprofAddr, Handler: pprofHandler}
	go server.ListenAndServe()
}
