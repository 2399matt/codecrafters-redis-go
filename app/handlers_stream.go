package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func handleXadd(c *Client, value Value) string {
	if len(value.Array) < 5 {
		return encodeError("ERR missing arguments for 'XADD'")
	}
	key := value.Array[1].Str
	req, err := parseID(value.Array[2].Str)
	if err != nil {
		return encodeError(err.Error())
	}
	value.Array = value.Array[3:]
	entries := make([]StreamField, 0)
	for i := 0; i < len(value.Array)-1; i++ {
		k := value.Array[i].Str
		v := value.Array[i+1].Str
		entries = append(entries, StreamField{key: k, value: v})
	}
	if id, err := c.server.storage.xAdd(key, req, entries); err != nil {
		return encodeError(err.Error())
	} else {
		return encodeBulkString(fmt.Sprintf("%d-%d", id.ms, id.seq))
	}
}

func handleXrange(c *Client, value Value) string {
	if len(value.Array) != 4 {
		return encodeError("ERR invalid syntax for 'XRANGE'")
	}
	key := value.Array[1].Str
	start, err := parseRangeID(value.Array[2].Str, true)
	if err != nil {
		return encodeError(err.Error())
	}
	end, err := parseRangeID(value.Array[3].Str, false)
	if err != nil {
		return encodeError(err.Error())
	}
	streamEntries, err := c.server.storage.xRange(key, start, end)
	if err != nil {
		return encodeError(err.Error())
	}
	values := make([]Value, 0)
	for _, entry := range streamEntries {
		values = append(values, encodeStreamEntry(entry))
	}
	return encodeArray(values)
}

func handleXread(c *Client, value Value) string {
	isBlocking := false
	var xreads []XReadResult
	var waiting chan struct{} = nil
	var limit float64
	var err error
	streamsIdx := -1
	if strings.ToUpper(value.Array[1].Str) == "BLOCK" {
		isBlocking = true
		limit, err = strconv.ParseFloat(value.Array[2].Str, 64)
		if err != nil {
			return encodeError("ERR invalid limit set for 'BLOCK'")
		}
		fmt.Printf("BLOCK SETUP FOR DURATION: %.2f\n", limit)
	}
	for i, v := range value.Array {
		if strings.ToUpper(v.Str) == "STREAMS" {
			streamsIdx = i
			break
		}
	}
	if streamsIdx == -1 {
		return encodeError("ERR syntax error")
	}
	rest := value.Array[streamsIdx+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return encodeError("ERR Unbalanced XREAD list")
	}
	n := len(rest) / 2
	keyTokens := rest[:n]
	idTokens := rest[n:]
	queries := make([]XReadQuery, 0)
	for i := range n {
		id, err := parseRangeID(idTokens[i].Str, true)
		if err != nil {
			return encodeError(err.Error())
		}
		if id.needLastEntry {
			id = c.server.storage.getLastStreamID(keyTokens[i].Str)
		}
		queries = append(queries, XReadQuery{key: keyTokens[i].Str, id: id})
	}
	if isBlocking {
		waiting = make(chan struct{}, 1)
		xreads = c.server.storage.xRead(waiting, queries)
	} else {
		xreads = c.server.storage.xRead(nil, queries)
	}
	fmt.Printf("LENGTH OF xreads: %d\n", len(xreads))
	if len(xreads) == 0 && isBlocking {
		keys := keysFromQueries(queries)
		if limit == 0 {
			<-waiting
			xreads = c.server.storage.xRead(nil, queries)
			c.server.storage.swPool.removeWaiter(waiting, keys)
		} else {
			timeout := time.After(time.Duration(limit * float64(time.Millisecond)))
			select {
			case <-waiting:
				xreads = c.server.storage.xRead(nil, queries)
				c.server.storage.swPool.removeWaiter(waiting, keys)
				fmt.Printf("Reads found: %d\n", len(xreads))
				break
			case <-timeout:
				c.server.storage.swPool.removeWaiter(waiting, keys)
				fmt.Printf("Timeout reached\n")
				return encodeNullArray()
			}
		}
	}
	values := make([]Value, 0)
	for _, read := range xreads {
		values = append(values, encodeXReadResult(read))
	}
	return encodeArray(values)
}
