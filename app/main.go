package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

//var storage = NewStorage()

type Server struct {
	storage    *Storage
	isReplica  bool
	config     *Config
	replicas   []*Replica
	mu         *sync.Mutex
	replOffset int64
}

type Replica struct {
	conn      net.Conn
	listening bool
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
		replicas:  make([]*Replica, 0),
		mu:        &sync.Mutex{},
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
	parser := NewParser(bufio.NewReader(conn))
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
	val, err := parser.parse()
	if err != nil || val.Str != "PONG" {
		fmt.Printf("no PONG from master: %v", err)
		os.Exit(1)
	}
	replConf := []Value{
		{
			Type: BulkString,
			Str:  "REPLCONF",
		},
		{
			Type: BulkString,
			Str:  "listening-port",
		},
		{
			Type: BulkString,
			Str:  s.config.port,
		},
	}
	if _, err := conn.Write([]byte(encodeArray(replConf))); err != nil {
		fmt.Printf("unable to send replconf: %v", err)
		os.Exit(1)
	}
	val, err = parser.parse()
	if err != nil {
		fmt.Printf("no OK from master: %v", err)
		os.Exit(1)
	}
	if val.Type == SimpleString && val.Str == "OK" {
		replConf = []Value{
			{
				Type: BulkString,
				Str:  "REPLCONF",
			},
			{
				Type: BulkString,
				Str:  "capa",
			},
			{
				Type: BulkString,
				Str:  "psync2",
			},
		}
		if _, err := conn.Write([]byte(encodeArray(replConf))); err != nil {
			fmt.Printf("unable to send replconf: %v", err)
			os.Exit(1)
		}
		val, err = parser.parse()
		if err != nil || val.Str != "OK" {
			fmt.Printf("no OK from master: %v", err)
			os.Exit(1)
		}
		psync := []Value{
			{
				Type: BulkString,
				Str:  "PSYNC",
			},
			{
				Type: BulkString,
				Str:  "?",
			},
			{
				Type: BulkString,
				Str:  "-1",
			},
		}
		if _, err := conn.Write([]byte(encodeArray(psync))); err != nil {
			fmt.Printf("unable to send PSYNC to master: %v", err)
			os.Exit(1)
		}
		if val, err = parser.parse(); err != nil {
			fmt.Printf("err on master write: %v", err)
			os.Exit(1)
		}
		if val.Type != SimpleString {
			fmt.Printf("no simple string from master\n")
			os.Exit(1)
		}
		err := parser.parseRDB()
		if err != nil {
			fmt.Printf("unable to parse RDB binary: %v", err)
			os.Exit(1)
		}
		go s.handleMaster(parser, conn)
		return
	}
}

func (s *Server) propagate(value Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, r := range s.replicas {
		if !r.listening {
			continue
		}
		r.conn.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
		if _, err := r.conn.Write([]byte(encodeArray(value.Array))); err != nil {
			s.replicas = slices.Delete(s.replicas, i, i+1)
			continue
		}
	}
}

func (s *Server) handleMaster(parser *Parser, conn net.Conn) {
	master := &Client{Conn: conn, server: s}
	defer conn.Close()
	for {
		val, err := parser.parse()
		if err != nil {
			fmt.Printf("Lost connection to master: %v", err)
			return
		}
		res := handleCommand(master, val)
		s.replOffset = parser.offset
		if needsResponse(val) {
			_, err := conn.Write([]byte(res))
			if err != nil {
				fmt.Printf("unable to write to master: %v", err)
				return
			}
		}
	}
}
