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
	default:
		return encodeError("ERR unknown command")
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
	storage.Set(key, val, expiration)
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
	return encodeBulkString(val)
}
