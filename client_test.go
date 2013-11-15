package kestrel

import (
	"bytes"
	"testing"
)

const kestrelTestServer = "localhost:2229"

func TestSimplePutAndGetToAndFromServer(t *testing.T) {
	client := NewClient(kestrelTestServer)

	item := []byte("Hello World")
	nItems, err := client.Put("queue1", item)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}
	if nItems != 1 {
		t.Fatalf("Did not write 1 item to the queue")
	}

	items, err := client.Get("queue1", 1, 0, 0)
	if err != nil {
		t.Fatalf("Error occured getting an item from the queue: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("Did not receive one and only one item from the queue")
	}
	if !bytes.Equal(item, items[0].Data) {
		t.Fatalf("Byte sequence differed from expected: %v", items[0])
	}
}

func TestRetryWithSuccess(t *testing.T) {
	client := NewClient("bogusserver:1", kestrelTestServer)

	item := []byte("Hello World")
	nItems, err := client.Put("queue1", item)
	if err != nil {
		t.Fatalf("Error occured putting an item onto the queue: %v", err)
	}
	if nItems != 1 {
		t.Fatalf("Did not write 1 item to the queue")
	}
}

func TestRetryWithFailure(t *testing.T) {
	client := NewClient("bogusserver:1", "bogusserver:1", kestrelTestServer)
	client.Retries = 1

	item := []byte("Hello World")
	nItems, err := client.Put("queue1", item)
	if err == nil {
		t.Fatalf("No error occurred even though we should have run out of retries")
	}
	if nItems != 0 {
		t.Fatalf("Wrote an item even though an error occurred")
	}
}
