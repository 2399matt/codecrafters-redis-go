package main

import "fmt"

func handlePublish(c *Client, value Value) string {
	if len(value.Array) < 3 {
		return encodeError("ERR invalid arguments for 'PUBLISH'")
	}
	cName := value.Array[1].Str
	aud := c.server.pubsub.publish(cName, value.Array[2].Str)
	return encodeInteger(aud)
}

func handleUnsubscribe(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'UNSUBSCRIBE'")
	}
	cName := value.Array[1].Str
	fmt.Printf("unsub for: %s", cName)
	if c.server.pubsub.unsubscribe(c, cName) {
		c.subCount--
	}
	if c.subCount == 0 {
		c.subMode = false
	}
	vals := []Value{
		{
			Type: BulkString,
			Str:  "unsubscribe",
		},
		{
			Type: BulkString,
			Str:  cName,
		},
		{
			Type: Integer,
			Num:  c.subCount,
		},
	}
	fmt.Printf("Vals: %#v", vals)
	return encodeArray(vals)
}

func handleSubscribe(c *Client, value Value) string {
	if len(value.Array) < 2 {
		return encodeError("ERR invalid arguments for 'SUBSCRIBE'")
	}
	cName := value.Array[1].Str
	if c.server.pubsub.subscribe(c, cName) {
		c.subCount++
	}
	vals := []Value{
		{
			Type: BulkString,
			Str:  "subscribe",
		},
		{
			Type: BulkString,
			Str:  cName,
		},
		{
			Type: Integer,
			Num:  c.subCount,
		},
	}
	// set c.subMode == true?
	c.subMode = true
	return encodeArray(vals)
}
