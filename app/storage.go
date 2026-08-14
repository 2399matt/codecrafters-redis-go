package main

import (
	"sync"
	"time"
)

type Entry struct {
	payload    string
	expiration time.Time
}

type Storage struct {
	mu    *sync.RWMutex
	table map[string]Entry
}

func NewStorage() *Storage {
	return &Storage{
		mu:    &sync.RWMutex{},
		table: make(map[string]Entry),
	}
}

func (s *Storage) Set(key, value string, expiration time.Time) {
	entry := Entry{
		payload:    value,
		expiration: expiration,
	}
	s.mu.Lock()
	s.table[key] = entry
	s.mu.Unlock()
}

func (s *Storage) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		return "", false
	}
	if !entry.expiration.IsZero() && time.Now().After(entry.expiration) {
		delete(s.table, key)
		return "", false
	}
	return entry.payload, true
}
