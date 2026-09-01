package main

import (
	"os"
	"path/filepath"
	"sync"
)

type AOF struct {
	config *AOFConfig
	file   *os.File
	mu     *sync.Mutex
}

type AOFConfig struct {
	enabled     bool
	fileName    string
	dir         string
	dirName     string
	appendfSync string
}

func NewAOF(cfg *AOFConfig) (*AOF, error) {
	aof := &AOF{
		config: cfg,
		mu:     &sync.Mutex{},
	}
	if !cfg.enabled {
		return aof, nil
	}
	path := filepath.Join(cfg.dir, cfg.dirName)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	fullPath := filepath.Join(path, cfg.fileName+".1.incr.aof")
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	aof.file = file
	return aof, nil
}
