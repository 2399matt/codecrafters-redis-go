package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

//var storage = NewStorage()

type Server struct {
	storage   *Storage
	isReplica bool
	config    *Config
}
type Config struct {
	role             string
	port             string
	masterReplID     string
	masterReplOffset int
	masterAddr       string
}

var server *Server

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
	config := createConfig(role, port)
	server = &Server{
		storage:   NewStorage(),
		isReplica: role != "master",
		config:    config,
	}
	if server.isReplica {
		server.initHandShake()
	}
	fmt.Printf("Listening on port: %s\n", port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("unable to accept client: %v\n", err)
			continue
		}
		client := &Client{Conn: conn, queue: make([]Value, 0), watchQueue: make(map[string]struct{}), server: server}
		go client.handleClient()
	}
}

func createConfig(role, port string) *Config {
	if role == "master" {
		return &Config{
			role:             role,
			port:             port,
			masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			masterReplOffset: 0,
		}
	}
	parts := strings.Split(role, " ")
	host := parts[0]
	nPort := parts[1]
	return &Config{
		role:       "slave",
		port:       port,
		masterAddr: fmt.Sprintf("%s:%s", host, nPort),
	}
}

func (s *Server) initHandShake() {
	conn, err := net.Dial("tcp", s.config.masterAddr)
	if err != nil {
		fmt.Printf("unable to reach master instance: %v", err)
		os.Exit(1)
	}
	req := []Value{
		{
			Type: BulkString,
			Str:  "PING",
		},
	}
	if _, err := conn.Write([]byte(encodeArray(req))); err != nil {
		fmt.Printf("unable to ping master: %v", err)
		os.Exit(1)
	}
}
