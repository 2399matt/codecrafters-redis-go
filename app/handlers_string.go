package main

import (
	"strconv"
	"strings"
	"time"
)

func handleSet(c *Client, value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR Missing arguments for SET")
	}
	key := value.Array[1].Str
	val := value.Array[2].Str
	var expiration time.Time
	if len(value.Array) > 3 {
		if len(value.Array) != 5 {
			return encodeError("ERR syntax error")
		}
		option := strings.ToUpper(value.Array[3].Str)
		switch option {
		case "EX":
			num, err := strconv.Atoi(value.Array[4].Str)
			if err != nil {
				return encodeError("ERR invalid expiration")
			}
			expiration = time.Now().Add(time.Duration(num) * time.Second)
		case "PX":
			num, err := strconv.Atoi(value.Array[4].Str)
			if err != nil {
				return encodeError("ERR invalid expiration")
			}
			expiration = time.Now().Add(time.Duration(num) * time.Millisecond)
		}
	}
	entry := Entry{
		Type:       StringType,
		payload:    val,
		expiration: expiration,
	}
	c.server.storage.Set(key, entry)
	return encodeSimpleString("OK")
}

func handleGet(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR missing arguments for GET")
	}
	val, ok := c.server.storage.Get(value.Array[1].Str)
	if !ok {
		return encodeNullString()
	}
	return encodeBulkString(val.payload)
}

func handleType(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR missing arguments for 'TYPE'")
	}
	key := value.Array[1].Str
	entry, ok := c.server.storage.Get(key)
	if !ok {
		return encodeSimpleString("none")
	}
	switch entry.Type {
	case StreamType:
		return encodeSimpleString("stream")
	case StringType:
		return encodeSimpleString("string")
	case ListType:
		return encodeSimpleString("list")
	}
	return encodeSimpleString("none")
}

func handleIncrement(c *Client, value Value) string {
	if len(value.Array) != 2 {
		return encodeError("ERR invalid arguments for 'INCR'")
	}
	val, err := c.server.storage.increment(value.Array[1].Str)
	if err != nil {
		return encodeError(err.Error())
	}
	return encodeInteger(val)
}
