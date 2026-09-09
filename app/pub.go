package main

import (
	"sync"
	"time"
)

type PubSub struct {
	mu       *sync.Mutex
	channels map[string]Channel
}

type Channel struct {
	subs []*Client
}

type Message struct {
	channelName string
	message     string
}

func NewPubSub() *PubSub {
	return &PubSub{
		mu:       &sync.Mutex{},
		channels: make(map[string]Channel),
	}
}

func (p *PubSub) subscribe(client *Client, name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	client.subCount++
	c, ok := p.channels[name]
	if !ok {
		subs := make([]*Client, 0)
		subs = append(subs, client)
		channel := Channel{subs: subs}
		p.channels[name] = channel
		return
	}
	c.subs = append(c.subs, client)
	p.channels[name] = c
}

func (p *PubSub) publish(channelName, msg string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.channels[channelName]
	if !ok {
		return 0
	}
	vals := []Value{
		{
			Type: BulkString,
			Str:  "message",
		},
		{
			Type: BulkString,
			Str:  channelName,
		},
		{
			Type: BulkString,
			Str:  msg,
		},
	}
	for _, c := range c.subs {
		c.writeMu.Lock()
		c.Conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 300))
		if _, err := c.Conn.Write([]byte(encodeArray(vals))); err != nil {
			c.Conn.Close()
			c.writeMu.Unlock()
			continue
		}
	}
	return len(c.subs)
}
