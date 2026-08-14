package main

import (
	"sync"
	"time"
)

type ValueType int

const (
	StringType ValueType = iota
	ListType
)

type Entry struct {
	Type       ValueType
	List       []string
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

func (s *Storage) Set(key string, entry Entry) {
	// entry := Entry{
	// 	payload:    value,
	// 	expiration: expiration,
	// }
	s.mu.Lock()
	s.table[key] = entry
	s.mu.Unlock()
}

func (s *Storage) Get(key string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		return Entry{}, false
	}
	if !entry.expiration.IsZero() && time.Now().After(entry.expiration) {
		delete(s.table, key)
		return Entry{}, false
	}
	return entry, true
}
