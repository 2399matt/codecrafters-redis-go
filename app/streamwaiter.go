package main

import (
	"slices"
	"sync"
)

type StreamWaiterPool struct {
	mu *sync.Mutex
	//Waiters []*StreamWaiter
	Waiters map[string][]chan struct{}
}

func NewStreamWaiterPool() *StreamWaiterPool {
	return &StreamWaiterPool{
		mu:      &sync.Mutex{},
		Waiters: make(map[string][]chan struct{}, 0),
	}
}

func (sw *StreamWaiterPool) alertAll(streamKey string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, ch := range sw.Waiters[streamKey] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (sw *StreamWaiterPool) removeWaiter(clientChan chan struct{}, keys []string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, key := range keys {
		clients := sw.Waiters[key]
		for i, c := range clients {
			if c == clientChan {
				clients = slices.Delete(sw.Waiters[key], i, i+1)
				break
			}
		}
		if len(sw.Waiters[key]) == 0 {
			delete(sw.Waiters, key)
		} else {
			sw.Waiters[key] = clients
		}
	}
}

func (sw *StreamWaiterPool) setWaiter(clientChan chan struct{}, streamKeys []string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, key := range streamKeys {
		sw.Waiters[key] = append(sw.Waiters[key], clientChan)
	}
}
