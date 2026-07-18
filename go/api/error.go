//
// error.go
//
package api

import (
    "errors"
)


var (
    ErrDuplicateKey = errors.New("duplicate key")
    ErrExpired      = errors.New("expired")
    ErrForbidden    = errors.New("forbidden")
)


