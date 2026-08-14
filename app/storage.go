package main

import (
	"fmt"
	"slices"
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
	s.mu.Lock()
	s.table[key] = entry
	s.mu.Unlock()
}

func (s *Storage) ListPush(isRight bool, key string, values []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		entry = Entry{
			Type: ListType,
			List: values,
		}
		s.table[key] = entry
		return len(values), nil
	}
	if entry.Type != ListType {
		return 0, fmt.Errorf("Invalid type, did not get list")
	}
	if isRight {
		entry.List = append(entry.List, values...)
	} else {
		slices.Reverse(values)
		entry.List = append(values, entry.List...)
	}
	s.table[key] = entry
	return len(entry.List), nil
}

func (s *Storage) ListPop(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok || len(entry.List) == 0 {
		return ""
	}
	res := entry.List[0]
	entry.List = entry.List[1:]
	s.table[key] = entry
	return res
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
