package main

import "net"

type Client struct {
	Conn net.Conn
	send chan string
}

func (c *Client) write() {
	defer c.Conn.Close()
	for msg := range c.send {
		_, err := c.Conn.Write([]byte(msg))
		if err != nil {
			break
		}
	}
}

func (c *Client) handleClient() {
	select {
	case c.send <- "+PONG\r\n":
	default:
	}
}
