package selfcustody

import "time"

type Address struct {
	ID          uint64
	ChainFamily string
	Address     string
	CreatedAt   time.Time
}

type Link struct {
	ID          uint64
	AddressID   uint64
	Subject     string
	DisplayName string
	LinkedAt    time.Time
	UnlinkedAt  *time.Time
}

type LinkChallenge struct {
	ID          uint64
	Subject     string
	BindingHash [32]byte
	ChainFamily string
	Address     string
	Nonce       [32]byte
	Message     []byte
	CreatedAt   time.Time
	ExpiresAt   time.Time
	ConsumedAt  *time.Time
}
