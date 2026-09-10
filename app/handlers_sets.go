package main

import "strconv"

type SortedSet struct {
	entries []*SortedEntry
}

type SortedEntry struct {
	score float64
	value string
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
