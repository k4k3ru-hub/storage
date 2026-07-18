//
// id.go
//
package generator

import (
    "sync"
    "time"
)


type ID struct {
    mu sync.Mutex
    id uint64
}


//
// Generate an ID.
//
// Version:
//   - 2026-07-18: Added.
//
func (i *ID) Generate() uint64 {
    i.mu.Lock()
    defer i.mu.Unlock()
    id := uint64(time.Now().UnixNano())
    if id <= i.id {
        id = i.id + 1
    }
    i.id = id
    return id
}
