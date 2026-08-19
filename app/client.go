package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

type XReadQuery struct {
	key string
	id  StreamID
}

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
		response := handleCommand(c, val)
		if _, err := c.Conn.Write([]byte(response)); err != nil {
			c.Conn.Close()
			return
		}
	}
}

func handleCommand(client *Client, value Value) string {
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
	case "LLEN":
		return handleLlen(value)
	case "LPOP":
		return handlePop(value)
	case "BLPOP":
		return client.handleBLpop(value)
	case "TYPE":
		return handleType(value)
	case "XADD":
		return handleXadd(value)
	case "XRANGE":
		return handleXrange(value)
	case "XREAD":
		return handleXread(value)
	default:
		return encodeError("ERR unknown command")
	}
}

// TODO when time support is added, we'll need to have a ticker.
// if time runs out, we'll call the cleanup (true/false)
// if false, then we have a value so we can branch off, otherwise close the channel immediately and reply with null

func (client *Client) handleBLpop(value Value) string {
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
	vals := storage.ListPop(waiting, 1, key)
	if len(vals) == 0 {
		if duration == 0 {
			val = <-waiting
		} else {
			timeout := time.After(time.Duration(duration * float64(time.Second)))
			select {
			case val = <-waiting:
				break
			case <-timeout:
				if storage.waiterPool.removeWaiter(waiting, key) {
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

func handlePop(value Value) string {
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
	popped := storage.ListPop(nil, toRemove, key)
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

func handleLlen(value Value) string {
	if len(value.Array) != 2 {
		return encodeError("ERR missing argument for 'LLEN'")
	}
	entry, ok := storage.Get(value.Array[1].Str)
	if !ok || entry.Type != ListType {
		return encodeInteger(0)
	}
	return encodeInteger(len(entry.List))
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

// only handling none/string for now
func handleType(value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR missing arguments for 'TYPE'")
	}
	key := value.Array[1].Str
	entry, ok := storage.Get(key)
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

func handleXadd(value Value) string {
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
	if id, err := storage.xAdd(key, req, entries); err != nil {
		return encodeError(err.Error())
	} else {
		return encodeBulkString(fmt.Sprintf("%d-%d", id.ms, id.seq))
	}
}

func handleXrange(value Value) string {
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
	streamEntries, err := storage.xRange(key, start, end)
	if err != nil {
		return encodeError(err.Error())
	}
	values := make([]Value, 0)
	for _, entry := range streamEntries {
		values = append(values, encodeStreamEntry(entry))
	}
	return encodeArray(values)
}

func handleXread(value Value) string {
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
		queries = append(queries, XReadQuery{key: keyTokens[i].Str, id: id})
	}
	if isBlocking {
		waiting = make(chan struct{}, 1)
		xreads = storage.xRead(waiting, queries)
	} else {
		xreads = storage.xRead(nil, queries)
	}
	fmt.Printf("LENGTH OF xreads: %d\n", len(xreads))
	if len(xreads) == 0 && isBlocking {
		waiting := make(chan struct{}, 1)
		timeout := time.After(time.Duration(limit * float64(time.Millisecond)))
		select {
		case <-waiting:
			xreads = storage.xRead(nil, queries)
			fmt.Printf("Reads found: %d\n", len(xreads))
			break
		case <-timeout:
			storage.swPool.removeWaiter(waiting)
			close(waiting)
			fmt.Printf("Timeout reached\n")
			return encodeNullArray()
		}
	}
	values := make([]Value, 0)
	for _, read := range xreads {
		values = append(values, encodeXReadResult(read))
	}
	return encodeArray(values)
}
