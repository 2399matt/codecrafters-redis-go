package main

import (
	"fmt"
	"math"
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
	fields []StreamField
}

type StreamField struct {
	key   string
	value string
}

type Stream struct {
	entries []StreamEntry
	lastID  StreamID
}

func (s *Stream) Add(req IDRequest, entries []StreamField) (StreamID, error) {
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

func parseRangeID(raw string, isStart bool) (StreamID, error) {
	if isStart && raw == "-" {
		return StreamID{0, 0}, nil
	}
	if !isStart && raw == "+" {
		return StreamID{math.MaxInt64, math.MaxInt64}, nil
	}
	parts := strings.Split(raw, "-")
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("ERR invalid value for ms")
	}
	if len(parts) == 1 {
		if isStart {
			return StreamID{ms: ms, seq: 0}, nil
		}
		return StreamID{ms: ms, seq: math.MaxInt64}, nil
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("ERR invalid value for seq")
	}
	return StreamID{ms: ms, seq: seq}, nil
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

func (s *Stream) xRange(start, end StreamID) []StreamEntry {
	res := make([]StreamEntry, 0)
	for _, entry := range s.entries {
		if idLess(entry.id, start) {
			continue
		}
		if idLess(end, entry.id) {
			break
		}
		res = append(res, entry)
	}
	return res
}

func idLess(a, b StreamID) bool {
	return a.ms < b.ms || (a.ms == b.ms && a.seq < b.seq)
}
