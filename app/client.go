package main

import (
	"net"
)

type Client struct {
	Conn net.Conn
}

func (c *Client) handleClient() {
	for {
		buf := make([]byte, 1024)
		n, err := c.Conn.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			c.Conn.Write([]byte("+PONG\r\n"))
		}
	}
}
