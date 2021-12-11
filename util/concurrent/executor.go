package concurrent

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
)

type WaitGroup struct {
	wg sync.WaitGroup
}

func NewWaitGroup() *WaitGroup {
	return &WaitGroup{wg: sync.WaitGroup{}}
}

func (ex *WaitGroup) Run(fn func()) {
	ex.wg.Add(1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("executor", err, string(debug.Stack()))
			}
			ex.wg.Done()
		}()
		fn()
	}()
}

func (ex *WaitGroup) RunC(ctx context.Context, fn func()) error {
	ex.wg.Add(1)
	sig := make(chan struct{})
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("executor.runc: panic", err, "\n", string(debug.Stack()))
			}
			sig <- struct{}{}
		}()
		fn()
	}()

	var err error
	select {
	case <-sig:
	case <-ctx.Done():
		err = ctx.Err()
	}
	ex.wg.Done()
	return err
}

func (ex *WaitGroup) Wait() {
	ex.wg.Wait()
}

func Go(fn func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("Go:", err, "\n", string(debug.Stack()))
			}
		}()
		fn()
	}()
}

func GoM(fn, pre, post func()) {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("GoM:", err, "\n", string(debug.Stack()))
			}
		}()
		if pre != nil {
			pre()
		}
		fn()
		if post != nil {
			post()
		}
	}()
}

func GoC(ctx context.Context, fn func()) (rerr error) {
	sig := make(chan struct{})
	go func() {
		defer func() {
			if err := recover(); err != nil {
				log.Println("GoC:", err, "\n", string(debug.Stack()))
			}
			sig <- struct{}{}
		}()
		fn()
	}()

	select {
	case <-sig:
	case <-ctx.Done():
		rerr = ctx.Err()
	}
	return
}
