package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

type XReadQuery struct {
	key string
	id  StreamID
}

// TODO Client may need a "submode", with a separate handler if flagged to only accept proper commands
type Client struct {
	writeMu    *sync.Mutex
	Conn       net.Conn
	isQueued   bool
	queue      []Value
	server     *Server
	watchQueue map[string]struct{}
	subMode    bool
	subCount   int
	replica    *Replica
	isReplay   bool
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
		fmt.Printf("Received: %#v\n", val)
		response := handleCommand(c, val)
		c.writeMu.Lock()
		if _, err := c.Conn.Write([]byte(response)); err != nil {
			c.Conn.Close()
			c.writeMu.Unlock()
			return
		}
		c.writeMu.Unlock()
	}
}

func handleCommand(c *Client, value Value) string {
	if value.Type != Array || len(value.Array) == 0 {
		return encodeError("ERR invalid command")
	}
	command := strings.ToUpper(value.Array[0].Str)
	if c.isQueued && command != "EXEC" && command != "MULTI" && command != "DISCARD" && command != "WATCH" {
		c.queue = append(c.queue, value)
		return encodeSimpleString("QUEUED")
	}
	isWrite := isWriteCommand(command)
	if c.subMode {
		return subModeRouter(c, value, command)
	}
	res := commandRouter(c, value, command)
	if !c.server.isReplica && isWrite && !strings.HasPrefix(res, string(Error)) {
		c.server.propagate(value)
	}
	if isWrite && !c.isReplay {
		if err := c.server.aof.appendEntry(encode(value)); err != nil {
			fmt.Printf("append aof failure: %v\n", err)
		}
	}
	return res
}

func subModeRouter(c *Client, value Value, command string) string {
	switch command {
	case "PING":
		vals := []Value{
			{
				Type: BulkString,
				Str:  "pong",
			},
			{
				Type: BulkString,
				Str:  "",
			},
		}
		return encodeArray(vals)
	case "SUBSCRIBE":
		return handleSubscribe(c, value)
	case "UNSUBSCRIBE":
		return handleUnsubscribe(c, value)
	default:
		return encodeError(fmt.Sprintf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", command))
	}
}

func commandRouter(c *Client, value Value, command string) string {
	switch command {
	case "PING":
		return encodeSimpleString("PONG")
	case "ECHO":
		return encodeBulkString(value.Array[1].Str)
	case "SET":
		return handleSet(c, value)
	case "GET":
		return handleGet(c, value)
	case "RPUSH", "LPUSH":
		return handleListPush(c, value)
	case "LRANGE":
		return handleLRange(c, value)
	case "LLEN":
		return handleLlen(c, value)
	case "LPOP":
		return handlePop(c, value)
	case "BLPOP":
		return handleBLpop(c, value)
	case "TYPE":
		return handleType(c, value)
	case "XADD":
		return handleXadd(c, value)
	case "XRANGE":
		return handleXrange(c, value)
	case "XREAD":
		return handleXread(c, value)
	case "INCR":
		return handleIncrement(c, value)
	case "MULTI":
		return handleMulti(c)
	case "EXEC":
		return handleExec(c)
	case "DISCARD":
		return handleDiscard(c)
	case "WATCH":
		return handleWatch(c, value)
	case "UNWATCH":
		return handleUnwatch(c)
	case "INFO":
		return handleInfo(c, value)
	case "REPLCONF":
		return handleReplConf(c, value)
	case "PSYNC":
		return handlePsync(c, value)
	case "WAIT":
		return handleWait(c, value)
	case "CONFIG":
		return handleConfig(c, value)
	case "KEYS":
		return handleKeys(c, value)
	case "SUBSCRIBE":
		return handleSubscribe(c, value)
	case "PUBLISH":
		return handlePublish(c, value)
	case "ZADD":
		return handleZAdd(c, value)
	case "ZRANK":
		return handleZRank(c, value)
	case "ZRANGE":
		return handleZRange(c, value)
	default:
		return encodeError("ERR unknown command")
	}
}

func isWriteCommand(command string) bool {
	switch command {
	case "SET", "RPUSH", "LPUSH", "LPOP", "BLPOP", "INCR", "XADD", "ZADD":
		return true
	default:
		return false
	}
}
