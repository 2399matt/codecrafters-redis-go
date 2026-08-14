package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	Conn net.Conn
}

func (c *Client) handleClient() {
	parser := NewParser(c.Conn)
	for {
		val, err := parser.parse()
		if err != nil {
			fmt.Printf("Parsing error: %v", err)
			c.Conn.Close()
			return
		}
		response := handleCommand(val)
		if _, err := c.Conn.Write([]byte(response)); err != nil {
			c.Conn.Close()
			return
		}
	}
}

func handleCommand(value Value) string {
	if value.Type != Array || len(value.Array) == 0 {
		return encodeError("ERR invalid command")
	}
	command := strings.ToUpper(value.Array[0].Str)
	switch command {
	case "PING":
		return encodeSimpleString("PONG")
	case "ECHO":
		return encodeBulkString(value.Array[1].Str)
	case "SET":
		return handleSet(value)
	case "GET":
		return handleGet(value)
	case "RPUSH", "LPUSH":
		return handleListPush(value)
	case "LRANGE":
		return handleLRange(value)
	default:
		return encodeError("ERR unknown command")
	}
}

func handleLRange(value Value) string {
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
	entry, ok := storage.Get(key)
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

func handleListPush(value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR invalid usage of 'RPUSH'")
	}
	cmd := value.Array[0].Str
	listName := value.Array[1].Str
	args := make([]string, 0)
	for i := 2; i < len(value.Array); i++ {
		args = append(args, value.Array[i].Str)
	}
	if length, err := storage.ListPush(cmd == "RPUSH", listName, args); err != nil {
		return encodeError("ERR invalid usage of RPUSH")
	} else {
		return encodeInteger(length)
	}

}

func handleSet(value Value) string {
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
	storage.Set(key, entry)
	return encodeSimpleString("OK")
}

func handleGet(value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR missing arguments for GET")
	}
	val, ok := storage.Get(value.Array[1].Str)
	if !ok {
		return encodeNullString()
	}
	return encodeBulkString(val.payload)
}
