package main

import "sync"

type PubSub struct {
	mu       *sync.Mutex
	channels map[string]Channel
}

type Channel struct {
	subs []*Client
}

func NewPubSub() *PubSub {
	return &PubSub{
		mu:       &sync.Mutex{},
		channels: make(map[string]Channel),
	}
}

func (p *PubSub) subscribe(client *Client, name string) Channel {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.channels[name]
	if !ok {
		subs := make([]*Client, 0)
		subs = append(subs, client)
		channel := Channel{subs: subs}
		p.channels[name] = channel
		return channel
	}
	c.subs = append(c.subs, client)
	p.channels[name] = c
	return c
}
