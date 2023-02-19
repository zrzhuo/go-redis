package sync

import (
	"sync"
	"time"
)

// Wait 对 sync.WaitGroup 的一个封装，在WaitGroup之上实现了超时功能
type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout 阻塞，直到Wait()结束，或者超时
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		w.wg.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return false // Wait()造成的阻塞已经结束
	case <-time.After(timeout):
		return true // 超时
	}
}
