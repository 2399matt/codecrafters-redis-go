package main

import (
	"slices"
	"sync"
)

type Waiter struct {
	ListName string
	Clients  []chan string
}

type WaiterPool struct {
	mu      *sync.Mutex
	Waiters []*Waiter
}

func NewWaiterPool() *WaiterPool {
	return &WaiterPool{
		mu:      &sync.Mutex{},
		Waiters: make([]*Waiter, 0),
	}
}

func (w *WaiterPool) getWaiter(listName string) chan string {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := 0; i < len(w.Waiters); i++ {
		waiter := w.Waiters[i]
		if waiter.ListName == listName && len(waiter.Clients) > 0 {
			waiterChan := waiter.Clients[0]
			waiter.Clients = waiter.Clients[1:]
			return waiterChan
		}
	}
	return nil
}

func (w *WaiterPool) removeWaiter(clientChan chan string, listName string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, wait := range w.Waiters {
		if wait.ListName == listName {
			for i, ch := range wait.Clients {
				if ch == clientChan {
					wait.Clients = slices.Delete(wait.Clients, i, i+1)
					return true
				}
			}
		}
	}
	return false
}

func (w *WaiterPool) setWaiter(clientChan chan string, listName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, waiter := range w.Waiters {
		if waiter.ListName == listName {
			waiter.Clients = append(waiter.Clients, clientChan)
			return
		}
	}
	clients := make([]chan string, 0)
	clients = append(clients, clientChan)
	waiter := &Waiter{ListName: listName, Clients: clients}
	w.Waiters = append(w.Waiters, waiter)
}
