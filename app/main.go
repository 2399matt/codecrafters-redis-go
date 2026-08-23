package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var storage = NewStorage()

func main() {
	var port string
	flag.StringVar(&port, "port", "6379", "port to run redis on")
	flag.Parse()
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", port)
		os.Exit(1)
	}
	fmt.Printf("Listening on port: %s\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("unable to accept client: %v\n", err)
			continue
		}
		client := &Client{Conn: conn, queue: make([]Value, 0), watchQueue: make(map[string]struct{})}
		go client.handleClient()
	}
}
