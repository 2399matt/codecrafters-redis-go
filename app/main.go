package main

import (
	"fmt"
	"net"
	"os"
)

var storage = NewStorage()

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("unable to accept client: %v", err)
			continue
		}
		client := &Client{Conn: conn, queue: make([]Value, 0), watchQueue: make(map[string]struct{})}
		go client.handleClient()
	}
}
