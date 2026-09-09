package main

import (
	"slices"
	"sync"
	"time"
)

type PubSub struct {
	mu       *sync.Mutex
	channels map[string]*Channel
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
		channels: make(map[string]*Channel),
	}
}

func (p *PubSub) subscribe(client *Client, name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.channels[name]
	if !ok {
		subs := make([]*Client, 0)
		subs = append(subs, client)
		channel := &Channel{subs: subs}
		p.channels[name] = channel
		return true
	}
	if !slices.Contains(c.subs, client) {
		c.subs = append(c.subs, client)
		return true
	}
	return false
}

func (p *PubSub) unsubscribe(client *Client, channelName string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	channels, ok := p.channels[channelName]
	if !ok {
		return false
	}
	for i, c := range channels.subs {
		if c == client {
			channels.subs = slices.Delete(channels.subs, i, i+1)
			if len(channels.subs) == 0 {
				delete(p.channels, channelName)
			}
			return true
		}
	}
	return false
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
		c.writeMu.Unlock()
	}
	return len(c.subs)
}
