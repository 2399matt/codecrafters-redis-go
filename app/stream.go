package main

type StreamID struct {
	seq int
	ms  int
}

type StreamEntry struct {
	id     string
	fields map[string]string
}

type Stream struct {
	entries []StreamEntry
}
