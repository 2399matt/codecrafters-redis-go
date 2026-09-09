package main

import (
	"fmt"
	"strconv"
	"time"
)

func handleBLpop(c *Client, value Value) string {
	if len(value.Array) != 3 {
		return encodeError("ERR invalid syntax for 'BLPOP'")
	}
	key := value.Array[1].Str
	duration, err := strconv.ParseFloat(value.Array[2].Str, 64)
	if err != nil {
		return encodeError("ERR invalid duration set for 'BLPOP'")
	}
	waiting := make(chan string, 1)
	val := ""
	vals := c.server.storage.ListPop(waiting, 1, key)
	if len(vals) == 0 {
		if duration == 0 {
			val = <-waiting
		} else {
			timeout := time.After(time.Duration(duration * float64(time.Second)))
			select {
			case val = <-waiting:
				break
			case <-timeout:
				if c.server.storage.waiterPool.removeWaiter(waiting, key) {
					return encodeNullArray()
				}
				val = <-waiting
			}
		}
	} else {
		val = vals[0]
	}
	values := []Value{
		{
			Type: BulkString,
			Str:  key,
		},
		{
			Type: BulkString,
			Str:  val,
		},
	}
	return encodeArray(values)
}

func handlePop(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR missing argument for 'LPOP'")
	}
	toRemove := 1
	var err error
	if len(value.Array) == 3 {
		toRemove, err = strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return encodeError("ERR invalid index for 'LPOP'")
		}
	}
	key := value.Array[1].Str
	popped := c.server.storage.ListPop(nil, toRemove, key)
	if popped == nil {
		return encodeNullString()
	}
	if len(popped) == 1 {
		return encodeBulkString(popped[0])
	}
	vals := make([]Value, 0, len(popped))
	for _, str := range popped {
		vals = append(vals, Value{Type: BulkString, Str: str})
	}
	return encodeArray(vals)
}

func handleLlen(c *Client, value Value) string {
	if len(value.Array) != 2 {
		return encodeError("ERR missing argument for 'LLEN'")
	}
	entry, ok := c.server.storage.Get(value.Array[1].Str)
	if !ok || entry.Type != ListType {
		return encodeInteger(0)
	}
	return encodeInteger(len(entry.List))
}

func handleLRange(c *Client, value Value) string {
	if len(value.Array) != 4 {
		return encodeError("ERR missing arguments for 'LRANGE'")
	}
	key := value.Array[1].Str
	var start, end int
	var err error
	if start, err = strconv.Atoi(value.Array[2].Str); err != nil {
		return encodeError("ERR invalid start index for 'LRANGE'")
	}
	if end, err = strconv.Atoi(value.Array[3].Str); err != nil {
		return encodeEmptyArray()
	}
	entry, ok := c.server.storage.Get(key)
	if !ok {
		return encodeEmptyArray()
	}
	length := len(entry.List)
	if start < 0 {
		start = max(length+start, 0)
	}
	if end < 0 {
		end = length + end
	}
	if start > end || end < 0 || start >= length {
		return encodeEmptyArray()
	}
	end = min(length-1, end)
	fmt.Printf("Start: %d   End: %d\n", start, end)
	values := make([]Value, 0)
	for i := start; i <= end; i++ {
		values = append(values, Value{Type: BulkString, Str: entry.List[i]})
	}
	return encodeArray(values)
}

func handleListPush(c *Client, value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR invalid usage of 'RPUSH'")
	}
	cmd := value.Array[0].Str
	listName := value.Array[1].Str
	args := make([]string, 0)
	for i := 2; i < len(value.Array); i++ {
		args = append(args, value.Array[i].Str)
	}
	if length, err := c.server.storage.ListPush(cmd == "RPUSH", listName, args); err != nil {
		return encodeError("ERR invalid usage of RPUSH")
	} else {
		return encodeInteger(length)
	}

}
