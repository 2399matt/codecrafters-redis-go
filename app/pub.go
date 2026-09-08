package main

import "sync"

type PubSub struct {
	mu       *sync.Mutex
	channels map[string]Channel
}

type Channel struct {
	subs []chan string
}

func NewPubSub() *PubSub {
	return &PubSub{
		mu:       &sync.Mutex{},
		channels: make(map[string]Channel),
	}
}

func (p *PubSub) subscribe(client chan string, name string) Channel {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.channels[name]
	if !ok {
		subs := make([]chan string, 64)
		subs = append(subs, client)
		channel := Channel{subs: subs}
		p.channels[name] = channel
		return channel
	}
	c.subs = append(c.subs, client)
	p.channels[name] = c
	return c
}
