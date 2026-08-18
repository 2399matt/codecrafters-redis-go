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
