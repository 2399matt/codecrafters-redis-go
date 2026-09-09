package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
)

var server *Server

func main() {
	var appendOnly bool
	var appendOnlyStr string
	var appendDirName string
	var appendfSync string
	var appendFileName string
	var port string
	var role string
	var dir string
	var dbFileName string
	flag.StringVar(&port, "port", "6379", "port to run redis instance on")
	flag.StringVar(&role, "replicaof", "master", "master or replication redis instance")
	flag.StringVar(&dir, "dir", "/app", "Directory for RDB file")
	flag.StringVar(&dbFileName, "dbfilename", "dump.rdb", "RDB file name")
	flag.StringVar(&appendDirName, "appenddirname", "appendonlydir", "The subdirectory under dir where AOF and manifest files are stored")
	flag.StringVar(&appendFileName, "appendfilename", "appendonly.aof", "The name of the append-only file that records write operations")
	flag.StringVar(&appendfSync, "appendfsync", "everysec", "How often buffered writes are flushed to the AOF file on disk")
	flag.StringVar(&appendOnlyStr, "appendonly", "no", "Controls whether AOF persistence is enabled or disabled")
	flag.Parse()
	if appendOnlyStr == "yes" {
		appendOnly = true
	}
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatalf("Failed to bind to port %s\n", port)
	}
	config := createConfig(role, port, dbFileName, dir)
	aofCfg := &AOFConfig{enabled: appendOnly, dir: dir, dirName: appendDirName, fileName: appendFileName, appendfSync: appendfSync}
	fmt.Printf("DIR NAME FOR AOF: %s\n", appendDirName)
	aof, err := NewAOF(aofCfg)
	if err != nil {
		log.Fatalf("unable to instantiate AOF: %v\n", err)
	}
	if aof.config.enabled {
		if err = aof.createManifest(); err != nil {
			log.Fatalf("unable to create manifest: %v\n", err)
		}
	}
	server = &Server{
		storage:   NewStorage(),
		isReplica: role != "master",
		config:    config,
		replicas:  make([]*Replica, 0),
		mu:        &sync.Mutex{},
		ackChan:   make(chan struct{}, 64),
		aof:       aof,
		pubsub:    NewPubSub(),
	}
	if server.isReplica {
		server.initHandShake()
	}
	if err = server.parseRDB(); err != nil {
		log.Fatalf("unable to load/create RDB file: %v\n", err)
	}
	fmt.Printf("Listening on port: %s\n", port)
	if server.aof.config.enabled {
		if err := server.handleLoadAOF(); err != nil {
			log.Fatalf("Failed to load from AOF: %v\n", err)
		}
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("unable to accept client: %v\n", err)
			continue
		}
		client := &Client{Conn: conn, queue: make([]Value, 0), watchQueue: make(map[string]struct{}), server: server, writeMu: &sync.Mutex{}}
		go client.handleClient()
	}
}
