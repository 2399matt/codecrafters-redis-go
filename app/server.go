package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type Server struct {
	storage    *Storage
	isReplica  bool
	config     *Config
	replicas   []*Replica
	mu         *sync.Mutex
	replOffset int64
	ackChan    chan struct{}
	aof        *AOF
	pubsub     *PubSub
}

type Replica struct {
	conn       net.Conn
	listening  bool
	replOffset int64
}

type Config struct {
	role         string
	port         string
	masterReplID string
	masterAddr   string
	dir          string
	dbFilename   string
}

func createConfig(role, port, dbFilename, dir string) *Config {
	if role == "master" {
		return &Config{
			role:         role,
			port:         port,
			masterReplID: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
			dbFilename:   dbFilename,
			dir:          dir,
		}
	}
	parts := strings.Split(role, " ")
	host := parts[0]
	nPort := parts[1]
	return &Config{
		role:       "slave",
		port:       port,
		masterAddr: fmt.Sprintf("%s:%s", host, nPort),
		dbFilename: dbFilename,
		dir:        dir,
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
	arr := []byte(encodeArray(value.Array))
	s.replOffset += int64(len(arr))
	for i, r := range s.replicas {
		if !r.listening {
			continue
		}
		r.conn.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
		if _, err := r.conn.Write(arr); err != nil {
			s.replicas = slices.Delete(s.replicas, i, i+1)
			continue
		}
	}
}

func (s *Server) sendGetAcks() {
	arr := []Value{
		{
			Type: BulkString,
			Str:  "REPLCONF",
		},
		{
			Type: BulkString,
			Str:  "GETACK",
		},
		{
			Type: BulkString,
			Str:  "*",
		},
	}
	ackReq := encodeArray(arr)
	for _, r := range s.replicas {
		_, err := r.conn.Write([]byte(ackReq))
		if err != nil {
			continue
		}
	}
}

func (s *Server) countAcks(offset int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, r := range s.replicas {
		// == breaking
		if r.replOffset >= offset {
			count++
		}
	}
	return count
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

func (s *Server) handleLoadAOF() error {
	// need a replay flag, getting infinite loop without it due to appendEntry being called in handleCommand
	c := &Client{
		Conn:     nil,
		isQueued: false,
		server:   s,
		isReplay: true,
	}
	full, err := s.aof.getFullPath()
	if err != nil {
		return err
	}
	file, err := os.Open(full)
	if err != nil {
		return err
	}
	parser := NewParser(file)
	for {
		val, err := parser.parse()
		if err != nil {
			break
		}
		fmt.Printf("RECEIVED VALUE FROM AOF FILE: %#v\n", val)
		handleCommand(c, val)
	}
	return nil
}

func needsResponse(value Value) bool {
	if len(value.Array) >= 2 {
		return value.Array[0].Str == "REPLCONF" && value.Array[1].Str == "GETACK"
	}
	return false
}
