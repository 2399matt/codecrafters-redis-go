package main

import (
	"fmt"
	"strconv"
)

type SortedSet struct {
	entries []*SortedEntry
}

type SortedEntry struct {
	score  float64
	member string
}

func handleZAdd(c *Client, value Value) string {
	if len(value.Array) < 4 {
		return encodeError("ERR invalid arguments for 'ZADD'")
	}
	setName := value.Array[1].Str
	score, err := strconv.ParseFloat(value.Array[2].Str, 64)
	if err != nil {
		return encodeError("ERR invalid score for member")
	}
	added := c.server.storage.zAdd(score, setName, value.Array[3].Str)
	return encodeInteger(added)
}

func handleZRank(c *Client, value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR invalid arguments for 'ZRANGE'")
	}
	setName := value.Array[1].Str
	member := value.Array[2].Str
	fmt.Printf("ZRANK REQ FOR SET: %s MEMBER: %s\n", setName, member)
	idx := c.server.storage.zRank(setName, member)
	if idx == -1 {
		return encodeNullString()
	}
	return encodeInteger(idx)
}
