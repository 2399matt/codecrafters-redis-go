package main

import "sync"

type Storage struct {
	mu    *sync.RWMutex
	table map[string]string
}

func NewStorage() *Storage {
	return &Storage{
		mu:    &sync.RWMutex{},
		table: make(map[string]string),
	}
}

func (s *Storage) Set(key, value string) {
	s.mu.Lock()
	s.table[key] = value
	s.mu.Unlock()
}

func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.table[key]
	return val, ok
}
