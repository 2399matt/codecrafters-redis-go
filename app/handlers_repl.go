package main

import (
	"fmt"
	"strconv"
	"time"
)

func handleWait(c *Client, value Value) string {
	if len(value.Array) != 3 {
		return encodeError("ERR invalid arguments for 'WAIT'")
	}
	target, err := strconv.Atoi(value.Array[1].Str)
	if err != nil {
		return encodeError("ERR invalid target for 'WAIT'")
	}
	if target == 0 {
		return encodeInteger(0)
	}
	timeout, err := strconv.Atoi(value.Array[2].Str)
	if err != nil {
		return encodeError("ERR invalid timeout for 'WAIT'")
	}
	c.server.sendGetAcks()
	currOffset := c.server.replOffset
	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	defer timer.Stop()
	for {
		total := c.server.countAcks(currOffset)
		if total >= target {
			return encodeInteger(total)
		}
		select {
		case <-c.server.ackChan:
		case <-timer.C:
			return encodeInteger(total)
		}
	}
}

// TODO Will probably need to add the actual repl offset here
func handlePsync(c *Client, value Value) string {
	resync := encodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", c.server.config.masterReplID))
	if c.replica != nil {
		c.replica.listening = true
	}
	return resync + encodeRDBFile(getEmptyRdb())
}

func handleReplConf(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'REPLCONF'")
	}
	cmd := value.Array[1].Str
	switch cmd {
	case "listening-port":
		replica := &Replica{conn: c.Conn}
		c.server.replicas = append(c.server.replicas, replica)
		c.replica = replica
	case "capa":
		break
	case "GETACK":
		if !c.server.isReplica {
			return encodeError("ERR not a valid replica")
		}
		val := Value{Type: Array, Array: []Value{
			{
				Type: BulkString,
				Str:  "REPLCONF",
			},
			{
				Type: BulkString,
				Str:  "ACK",
			},
			{
				Type: BulkString,
				Str:  strconv.FormatInt(c.server.replOffset, 10),
			},
		}}
		return encodeArray(val.Array)
	case "ACK":
		offset, err := strconv.Atoi(value.Array[2].Str)
		if err != nil {
			return encodeError("ERR invalid ACK offset")
		}
		c.replica.replOffset = int64(offset)
		select {
		case c.server.ackChan <- struct{}{}:
		default:
		}
		return ""
	default:
		return encodeError("ERR invalid arguments for 'REPLCONF'")
	}
	return encodeSimpleString("OK")
}
