package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TODO look this over, very fucking confusing.
// for full auto, ms = current time; seq = 0. (easier)

type StreamID struct {
	ms  int64
	seq int64
}

type IDRequest struct {
	ms       int64
	seq      int64
	fullAuto bool
	seqAuto  bool
}

type StreamEntry struct {
	id     StreamID
	fields map[string]string
}

type Stream struct {
	entries []StreamEntry
	lastID  StreamID
}

func (s *Stream) Add(req IDRequest, entries map[string]string) (StreamID, error) {
	id, err := s.createID(req)
	if err != nil {
		return StreamID{}, err
	}
	s.entries = append(s.entries, StreamEntry{
		id:     id,
		fields: entries,
	})
	s.lastID = id
	return id, nil
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

func parseID(raw string) (IDRequest, error) {
	if raw == "*" {
		return IDRequest{fullAuto: true}, nil
	}
	parts := strings.Split(raw, "-")
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return IDRequest{}, fmt.Errorf("ERR invalid stream id")
	}
	if parts[1] == "*" {
		return IDRequest{ms: ms, seqAuto: true}, nil
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return IDRequest{}, fmt.Errorf("ERR invalid stream id")
	}
	return IDRequest{ms: ms, seq: seq}, nil
}

func (s *Stream) createID(req IDRequest) (StreamID, error) {
	if req.fullAuto {
		ms := time.Now().UnixMilli()
		seq := int64(0)
		return StreamID{ms: ms, seq: seq}, nil
	}
	if req.seqAuto {
		var seq int64
		if len(s.entries) == 0 {
			if req.ms == 0 {
				seq = 1
			}
		}
		if req.ms == s.lastID.ms {
			seq = s.lastID.seq + 1
		}
		return StreamID{ms: req.ms, seq: seq}, nil
	}
	streamId := StreamID{ms: req.ms, seq: req.seq}
	if err := s.validateID(streamId); err != nil {
		return StreamID{}, err
	}
	return streamId, nil
}
