# go.kestrel

A simple wrapper over kestrel's thrift API. Includes support for multiple
servers and retries.

It's still early days, so dig into kestrel.go for all the details of what
you can do.

```go
import (
  "github.com/alindeman/go.kestrel"
  "time"
)

// assuming the thrift port is 2229
client := kestrel.NewClient("localhost:2229")

item := []byte("Hello World")
nitems, err := client.Put("queue1", [][]byte{item})

items, err := client.Get("queue1", 1, 0, 1*time.Minute)
```
