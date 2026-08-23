package main

import (
	"testing"
	"time"
)

func TestSeqGeneration(t *testing.T) {
	storage := NewStorage()
	req := IDRequest{ms: time.Now().UnixMilli(), seqAuto: true}
	key := "test"
	entries := []StreamField{
		StreamField{"foo", "bar"},
	}
	id, err := storage.xAdd(key, req, entries)
	if err != nil {
		t.Errorf("unexpected error from storage xAdd: %v", err)
	}
	if id.seq != 0 {
		t.Errorf("Expected sequence of new stream to start at zero, got %d", id.seq)
	}
}

func TestSetGet(t *testing.T) {
	key := "foo"
	entry := Entry{
		Type:    StringType,
		payload: "bar",
	}
	storage := NewStorage()
	storage.Set(key, entry)
	val, ok := storage.Get(key)
	if !ok {
		t.Errorf("key did not retrieve any value from storage")
	}
	if val.Type != StringType {
		t.Errorf("Invalid type on retrieved entry. Expected String, got %v", val.Type)
	}
	if val.payload != "bar" {
		t.Errorf("Invalid value on retrieved entry. Expected 'bar', got %s", val.payload)
	}
}

func TestStreamGet(t *testing.T) {
	store := NewStorage()
	key1 := "test1"
	key2 := "test2"
	req1 := IDRequest{
		ms:  0,
		seq: 0,
	}
	req2 := IDRequest{
		ms:  1,
		seq: 1,
	}
	store.xAdd(key1, req1, []StreamField{StreamField{"foo", "bar"}})
	store.xAdd(key2, req2, []StreamField{StreamField{"apple", "orange"}})
	if res1 := store.xRead(nil, []XReadQuery{XReadQuery{key1, StreamID{0, 0, false}}}); res1[0].entries[0].fields[0].value != "bar" {
		t.Errorf("Expected %s got %s", "bar", res1[0].entries[0].fields[0].value)
	}
}
