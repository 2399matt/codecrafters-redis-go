package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	// hardcoded increment file name val (not gonna stay)
	fullPath := filepath.Join(path, cfg.fileName+".1.incr.aof")
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	aof.file = file
	return aof, nil
}

func (a *AOF) createManifest() error {
	// (aofFileName).manifest
	// open file, and write file appendonly.aof.1.incr.aof seq 1 type i
	if a.file == nil {
		return fmt.Errorf("no AOF file to build manifest")
	}
	mPath := filepath.Join(a.config.dir, a.config.dirName)
	fullPath := filepath.Join(mPath, a.config.fileName+".manifest")
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write([]byte(fmt.Sprintf("file %s.1.incr.aof seq 1 type i", a.config.fileName))); err != nil {
		return err
	}
	return nil
}

func (a *AOF) getFileFromManifest() (string, error) {
	path := filepath.Join(a.config.dir, a.config.dirName)
	full := filepath.Join(path, a.config.fileName+".manifest")
	file, err := os.Open(full)
	if err != nil {
		return "", err
	}
	var filename string
	defer file.Close()
	scan := bufio.NewScanner(file)
	for scan.Scan() {
		line := scan.Text()
		parts := strings.Split(line, " ")
		filename = parts[1]
	}
	if err = scan.Err(); err != nil {
		return "", err
	}
	return filename, nil
}

func (a *AOF) appendEntry(resp string) error {
	filename, err := a.getFileFromManifest()
	if err != nil {
		return err
	}
	path := filepath.Join(a.config.dir, a.config.dirName)
	full := filepath.Join(path, filename)
	file, err := os.OpenFile(full, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err := file.Write([]byte(resp)); err != nil {
		return err
	}
	return nil
}
