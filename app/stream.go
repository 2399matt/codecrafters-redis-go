package main

import "fmt"

type StreamID struct {
	ms  int64
	seq int64
}

type StreamEntry struct {
	id     StreamID
	fields map[string]string
}

type Stream struct {
	entries []StreamEntry
	lastID  StreamID
}

func (s *Stream) Add(id StreamID, entries map[string]string) error {
	if err := s.validateID(id); err != nil {
		return err
	}
	s.entries = append(s.entries, StreamEntry{
		id:     id,
		fields: entries,
	})
	s.lastID = id
	return nil
}

func (s *Stream) validateID(id StreamID) error {
	if id.ms == 0 && id.seq == 0 {
		return fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}
	if id.ms < s.lastID.ms || (id.ms == s.lastID.ms && id.seq <= s.lastID.seq) {
		return fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	return nil
}
