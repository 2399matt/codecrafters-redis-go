package main

import (
	"slices"
	"sync"
)

type StreamWaiter struct {
	StreamKeys []string
	Clients    []chan struct{}
}

type StreamWaiterPool struct {
	mu      *sync.Mutex
	Waiters []*StreamWaiter
}

func NewStreamWaiterPool() *StreamWaiterPool {
	return &StreamWaiterPool{
		mu:      &sync.Mutex{},
		Waiters: make([]*StreamWaiter, 0),
	}
}

func (sw *StreamWaiterPool) alertAll(streamKey string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, w := range sw.Waiters {
		if slices.Contains(w.StreamKeys, streamKey) {
			for _, ch := range w.Clients {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (sw *StreamWaiterPool) removeWaiter(clientChan chan struct{}) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, wait := range sw.Waiters {
		for i, c := range wait.Clients {
			if c == clientChan {
				wait.Clients = slices.Delete(wait.Clients, i, i+1)
			}
		}
	}
}

func (sw *StreamWaiterPool) setWaiter(clientChan chan struct{}, streamKeys []string) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, key := range streamKeys {
		for _, w := range sw.Waiters {
			if slices.Contains(w.StreamKeys, key) {
				w.Clients = append(w.Clients, clientChan)
			}
		}
	}
	clients := make([]chan struct{}, 0)
	clients = append(clients, clientChan)
	waiter := &StreamWaiter{StreamKeys: streamKeys, Clients: clients}
	sw.Waiters = append(sw.Waiters, waiter)
}
