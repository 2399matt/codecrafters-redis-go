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
	StreamType
)

type Entry struct {
	Type       ValueType
	List       []string
	payload    string
	expiration time.Time
	stream     *Stream // populate on Type == StreamType
}

type Storage struct {
	waiterPool *WaiterPool
	mu         *sync.RWMutex
	table      map[string]Entry
}

func NewStorage() *Storage {
	return &Storage{
		mu:         &sync.RWMutex{},
		table:      make(map[string]Entry),
		waiterPool: NewWaiterPool(),
	}
}

func (s *Storage) Set(key string, entry Entry) {
	s.mu.Lock()
	s.table[key] = entry
	s.mu.Unlock()
}

// func (s *Storage) BLpop(clientChan chan bool, key string) string {

// }

// TODO In here: rather than checkforwaiters, we can have a for loop on the values slice
// we can constantly grab for a waiter in that loop, and send the value to the waiter. If no waiter: add to the entry list as normal
// Then just set to storage as before.
func (s *Storage) ListPush(isRight bool, key string, values []string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		entry = Entry{
			Type: ListType,
			List: make([]string, 0),
		}
		s.table[key] = entry
	} else if entry.Type != ListType {
		return 0, fmt.Errorf("Invalid type, did not get list")
	}
	if isRight {
		entry.List = append(entry.List, values...)
	} else {
		slices.Reverse(values)
		entry.List = append(values, entry.List...)
	}
	replyLen := len(entry.List)
	for len(entry.List) > 0 {
		waiter := s.waiterPool.getWaiter(key)
		if waiter == nil {
			break
		}
		waiter <- entry.List[0]
		entry.List = entry.List[1:]
	}
	s.table[key] = entry
	return replyLen, nil
}

func (s *Storage) ListPop(clientBLchan chan string, toRemove int, key string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok || len(entry.List) == 0 {
		if clientBLchan != nil {
			s.waiterPool.setWaiter(clientBLchan, key)
		}
		return nil
	}
	toRemove = min(len(entry.List), toRemove)
	popped := make([]string, 0, toRemove)
	for i := 0; i < toRemove; i++ {
		popped = append(popped, entry.List[i])
	}
	entry.List = entry.List[toRemove:]
	s.table[key] = entry
	return popped
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

func (s *Storage) xAdd(key string, req IDRequest, entries map[string]string) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		entry = Entry{
			Type:   StreamType,
			stream: &Stream{entries: make([]StreamEntry, 0), lastID: StreamID{0, 0}},
		}
		s.table[key] = entry
	}
	if entry.Type != StreamType {
		return StreamID{}, fmt.Errorf("Incorrect entry type")
	}
	return entry.stream.Add(req, entries)
}
