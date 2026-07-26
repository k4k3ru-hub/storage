//
// integer.go
//
package sqlscan

import (
    "fmt"
    "strconv"
)


//
// Scan value as uint8.
//
// Version:
//   - 2026-07-26: Added.
//
func Uint8(value any) (uint8, error) {
    switch v := value.(type) {
    case int:
        if v < 0 || uint64(v) > uint64(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case int8:
        if v < 0 {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case int16:
        if v < 0 || v > int16(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case int32:
        if v < 0 || v > int32(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case int64:
        if v < 0 || v > int64(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case uint:
        if uint64(v) > uint64(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case uint8:
        return v, nil

    case uint16:
        if v > uint16(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case uint32:
        if v > uint32(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case uint64:
        if v > uint64(^uint8(0)) {
            return 0, fmt.Errorf("out of range: value=%d", v)
        }
        return uint8(v), nil

    case []byte:
        return parseUint8(string(v))

    case string:
        return parseUint8(v)

    case nil:
        return 0, fmt.Errorf("value=null")

    default:
        return 0, fmt.Errorf("unsupported value type: type=%T", value)
    }
}

//
// Parse value as uint8.
//
// Version:
//   - 2026-07-26: Added.
//
func parseUint8(value string) (uint8, error) {
    parsed, err := strconv.ParseUint(value, 10, 8)
    if err != nil {
        return 0, fmt.Errorf("invalid value=%q: %w", value, err)
    }

    return uint8(parsed), nil
}
