package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var storage = NewStorage()

type Config struct {
	role string
	port string
}

var config *Config

func main() {
	var port string
	var role string
	flag.StringVar(&port, "port", "6379", "port to run redis instance on")
	flag.StringVar(&role, "replicaof", "master", "master or replication redis instance")
	flag.Parse()
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", port)
		os.Exit(1)
	}
	config = createConfig(role, port)
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

func createConfig(role, port string) *Config {
	if role == "master" {
		return &Config{
			role: role,
			port: port,
		}
	}
	// don't need to have the addr of the master yet
	//parts := strings.Split(role, " ")
	return &Config{
		role: "slave",
		port: port,
	}
}
