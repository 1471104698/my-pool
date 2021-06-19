package main

import "sync"

func main() {
	cond := sync.NewCond(&sync.Mutex{})
	cond.Broadcast()
}
