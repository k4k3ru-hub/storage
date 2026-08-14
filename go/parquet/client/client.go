//
// client.go
//
package client

import (
	"fmt"

	"github.com/k4k3ru-hub/storage/go/parquet/store"
)

type Params struct {
	Store store.Store
}

type Client struct {
	store store.Store
}

// New creates a Parquet client.
//
// Version:
//   - 2026-08-14: Added.
func New(params Params) (*Client, error) {
	if params.Store == nil {
		return nil, fmt.Errorf("parquet client store is nil")
	}
	return &Client{store: params.Store}, nil
}

// Store returns the configured object store.
//
// Version:
//   - 2026-08-14: Added.
func (c *Client) Store() store.Store {
	return c.store
}
