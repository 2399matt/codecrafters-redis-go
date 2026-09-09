package main

import (
	"fmt"
	"strings"
)

func handleUnwatch(c *Client) string {
	clear(c.watchQueue)
	return encodeSimpleString("OK")
}

func handleWatch(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'WATCH'")
	}
	if c.isQueued {
		return encodeError("ERR WATCH inside MULTI is not allowed")
	}
	keys := make([]string, 0, len(value.Array)-1)
	for _, val := range value.Array {
		keys = append(keys, val.Str)
	}
	c.server.storage.addWatchKeys(keys)
	for _, key := range keys {
		c.watchQueue[key] = struct{}{}
	}
	return encodeSimpleString("OK")
}

func handleDiscard(c *Client) string {
	defer clear(c.watchQueue)
	if !c.isQueued {
		return encodeError("ERR DISCARD without MULTI")
	}
	c.isQueued = false
	c.queue = c.queue[:0]
	return encodeSimpleString("OK")
}

func handleExec(c *Client) string {
	defer clear(c.watchQueue)
	if !c.isQueued {
		return encodeError("ERR EXEC without MULTI")
	}
	c.isQueued = false
	queue := c.queue
	c.queue = c.queue[:0]
	fmt.Printf("EXEC called with %d commands in queue\n", len(queue))
	if len(queue) == 0 {
		return encodeEmptyArray()
	}
	var result strings.Builder
	result.WriteString(fmt.Sprintf("*%d\r\n", len(queue)))
	for _, val := range queue {
		for k := range c.watchQueue {
			if !c.server.storage.checkWatchQueue(k) {
				return encodeNullArray()
			}
		}
		result.WriteString(handleCommand(c, val))
	}
	return result.String()
}

func handleMulti(c *Client) string {
	if c.isQueued {
		return encodeError("ERR MULTI calls cannot be nested")
	}
	c.isQueued = true
	return encodeSimpleString("OK")
}
