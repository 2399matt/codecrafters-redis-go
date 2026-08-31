package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
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
	stream     *Stream
}

type Storage struct {
	swPool     *StreamWaiterPool
	waiterPool *WaiterPool
	mu         *sync.RWMutex
	table      map[string]Entry
	watchkeys  map[string]struct{}
}

func NewStorage() *Storage {
	return &Storage{
		swPool:     NewStreamWaiterPool(),
		mu:         &sync.RWMutex{},
		table:      make(map[string]Entry),
		waiterPool: NewWaiterPool(),
		watchkeys:  make(map[string]struct{}),
	}
}

func (s *Storage) Set(key string, entry Entry) {
	s.mu.Lock()
	s.table[key] = entry
	delete(s.watchkeys, key)
	s.mu.Unlock()
}

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
	// need to capture length before alerting waiters
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
	delete(s.watchkeys, key)
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
	delete(s.watchkeys, key)
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

func (s *Storage) xAdd(key string, req IDRequest, entries []StreamField) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		entry = Entry{
			Type:   StreamType,
			stream: &Stream{entries: make([]StreamEntry, 0), lastID: StreamID{ms: 0, seq: 0}},
		}
		s.table[key] = entry
	} else if entry.Type != StreamType {
		return StreamID{}, fmt.Errorf("Incorrect entry type")
	}
	s.swPool.alertAll(key)
	delete(s.watchkeys, key)
	return entry.stream.Add(req, entries)
}

func (s *Storage) xRange(key string, start, end StreamID) ([]StreamEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		return []StreamEntry{}, nil
	}
	if entry.Type != StreamType {
		return nil, fmt.Errorf("ERR mismatched key for stream operation")
	}
	return entry.stream.xRange(start, end), nil
}

// TODO For the BLOCK param, we only need ONE stream to populate a value.
// Need a way to register the client as a waiter for EACH key given, and return on the first key that wakes up
// Still need to unregister the waiter from ALL the keys when returning though.
func (s *Storage) xRead(clientChan chan struct{}, queries []XReadQuery) []XReadResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]XReadResult, 0)
	for _, query := range queries {
		entry, ok := s.table[query.key]
		if !ok {
			continue
		}
		xReadRes := entry.stream.xRead(query.key, query.id)
		if len(xReadRes.entries) > 0 {
			results = append(results, xReadRes)
		}
	}
	if len(results) == 0 && clientChan != nil {
		keys := keysFromQueries(queries)
		s.swPool.setWaiter(clientChan, keys)
	}
	return results
}

func (s *Storage) increment(key string) (int, error) {
	var val int
	var err error
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.table[key]
	if !ok {
		val = 1
		entry = Entry{
			Type:    StringType,
			payload: fmt.Sprintf("%d", val),
		}
		s.table[key] = entry
	}
	if entry.Type != StringType {
		return 0, fmt.Errorf("ERR value is not an integer or out of range")
	}
	if ok {
		val, err = strconv.Atoi(entry.payload)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		val++
		entry.payload = fmt.Sprintf("%d", val)
		s.table[key] = entry
		delete(s.watchkeys, key)
	}
	return val, nil
}

func (s *Storage) getLastStreamID(streamKey string) StreamID {
	entry, ok := s.table[streamKey]
	if ok {
		return entry.stream.lastID
	}
	return StreamID{}
}

func (s *Storage) addWatchKeys(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		s.watchkeys[key] = struct{}{}
	}
}

func (s *Storage) getKeys(target string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]string, 0, len(s.table))
	for k := range s.table {
		if strings.HasPrefix(k, target) {
			res = append(res, k)
		}
	}
	return res
}

func (s *Storage) checkWatchQueue(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.watchkeys[key]
	return ok
}

func keysFromQueries(queries []XReadQuery) []string {
	keys := make([]string, 0, len(queries))
	for i := range queries {
		keys = append(keys, queries[i].key)
	}
	return keys
}
