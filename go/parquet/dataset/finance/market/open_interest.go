// open_interest.go
package market

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type OpenInterestUnit string

const (
	OpenInterestUnitBaseAsset     OpenInterestUnit = "base_asset"
	OpenInterestUnitContracts     OpenInterestUnit = "contracts"
	OpenInterestUnitQuoteNotional OpenInterestUnit = "quote_notional"
)

type OpenInterestPriceType string

const (
	OpenInterestPriceTypeMark          OpenInterestPriceType = "mark"
	OpenInterestPriceTypeIndex         OpenInterestPriceType = "index"
	OpenInterestPriceTypeOracle        OpenInterestPriceType = "oracle"
	OpenInterestPriceTypeVenueReported OpenInterestPriceType = "venue_reported"
)

type ContractSizeUnit string

const (
	ContractSizeUnitBaseAsset     ContractSizeUnit = "base_asset"
	ContractSizeUnitQuoteNotional ContractSizeUnit = "quote_notional"
)

// OpenInterest is one normalized open-interest observation.
type OpenInterest struct {
	// EventTimestamp is when the venue generated or published the observation.
	// When a venue provides no observation timestamp, the normalizer must use
	// ReceivedTimestamp and document that fallback at the integration boundary.
	EventTimestamp time.Time
	// ReceivedTimestamp is when K4K3RU received the observation.
	ReceivedTimestamp time.Time
	// RawQuantity is the unconverted open-interest value reported by the venue.
	// It is a float64 optimized for analytics and is not an exact accounting value.
	RawQuantity float64
	// RawUnit identifies the unit of RawQuantity.
	RawUnit OpenInterestUnit
	// Quantity is open interest normalized into base-asset units. It is a
	// float64 optimized for analytics and is not an exact accounting value.
	Quantity float64
	// NotionalValue is open interest normalized into NotionalCurrency units. It
	// is a float64 optimized for analytics and is not an exact accounting value.
	NotionalValue float64
	// NotionalCurrency identifies the currency of NotionalValue.
	NotionalCurrency string
	// ConversionPrice is the optional price used to derive normalized values.
	ConversionPrice *float64
	// ConversionPriceType identifies the source semantics of ConversionPrice.
	// It must be empty when ConversionPrice is absent.
	ConversionPriceType OpenInterestPriceType
	// ConversionPriceTimestamp is the observation time of ConversionPrice.
	// It must be present exactly when ConversionPrice is present.
	ConversionPriceTimestamp *time.Time
	// ContractSize is the venue contract multiplier used when RawQuantity is
	// contract-denominated. It is a float64 optimized for analytics.
	ContractSize *float64
	// ContractSizeUnit identifies whether ContractSize is base-asset quantity
	// or quote notional per contract.
	ContractSizeUnit *ContractSizeUnit
	// ContractSizeCurrency identifies the asset or currency of ContractSize.
	ContractSizeCurrency *string
}

// Validate validates a normalized open-interest observation.
//
// Returns:
//   - Validation error when a required value is missing or invalid.
//
// Version:
//   - 2026-08-26: Added.
func (r OpenInterest) Validate() error {
	if r.EventTimestamp.IsZero() {
		return fmt.Errorf("failed to validate open interest: event_timestamp=empty")
	}
	if r.ReceivedTimestamp.IsZero() {
		return fmt.Errorf("failed to validate open interest: received_timestamp=empty")
	}
	if !validNonNegativeOpenInterestNumber(r.RawQuantity) {
		return fmt.Errorf("failed to validate open interest: raw_quantity=out_of_range min_value=0")
	}
	switch r.RawUnit {
	case OpenInterestUnitBaseAsset, OpenInterestUnitContracts, OpenInterestUnitQuoteNotional:
	default:
		return fmt.Errorf("failed to validate open interest: raw_unit=invalid")
	}
	if !validNonNegativeOpenInterestNumber(r.Quantity) {
		return fmt.Errorf("failed to validate open interest: quantity=out_of_range min_value=0")
	}
	if !validNonNegativeOpenInterestNumber(r.NotionalValue) {
		return fmt.Errorf("failed to validate open interest: notional_value=out_of_range min_value=0")
	}
	if strings.TrimSpace(r.NotionalCurrency) == "" {
		return fmt.Errorf("failed to validate open interest: notional_currency=empty")
	}
	if r.ConversionPrice == nil {
		if r.ConversionPriceType != "" {
			return fmt.Errorf("failed to validate open interest: conversion_price=null conversion_price_type=invalid")
		}
		if r.ConversionPriceTimestamp != nil {
			return fmt.Errorf("failed to validate open interest: conversion_price=null conversion_price_timestamp=invalid")
		}
	} else {
		if math.IsNaN(*r.ConversionPrice) || math.IsInf(*r.ConversionPrice, 0) || *r.ConversionPrice <= 0 {
			return fmt.Errorf("failed to validate open interest: conversion_price=out_of_range min_value=0")
		}
		switch r.ConversionPriceType {
		case OpenInterestPriceTypeMark, OpenInterestPriceTypeIndex, OpenInterestPriceTypeOracle, OpenInterestPriceTypeVenueReported:
		default:
			return fmt.Errorf("failed to validate open interest: conversion_price_type=invalid")
		}
		if r.ConversionPriceTimestamp == nil {
			return fmt.Errorf("failed to validate open interest: conversion_price_timestamp=null")
		}
		if r.ConversionPriceTimestamp.IsZero() {
			return fmt.Errorf("failed to validate open interest: conversion_price_timestamp=empty")
		}
	}
	if r.ContractSize == nil {
		if r.ContractSizeUnit != nil {
			return fmt.Errorf("failed to validate open interest: contract_size=null contract_size_unit=invalid")
		}
		if r.ContractSizeCurrency != nil {
			return fmt.Errorf("failed to validate open interest: contract_size=null contract_size_currency=invalid")
		}
		if r.RawUnit == OpenInterestUnitContracts {
			return fmt.Errorf("failed to validate open interest: contract_size=null raw_unit=%q", r.RawUnit)
		}
	} else {
		if math.IsNaN(*r.ContractSize) || math.IsInf(*r.ContractSize, 0) || *r.ContractSize <= 0 {
			return fmt.Errorf("failed to validate open interest: contract_size=out_of_range min_value=0")
		}
		if r.ContractSizeUnit == nil {
			return fmt.Errorf("failed to validate open interest: contract_size_unit=null")
		}
		switch *r.ContractSizeUnit {
		case ContractSizeUnitBaseAsset, ContractSizeUnitQuoteNotional:
		default:
			return fmt.Errorf("failed to validate open interest: contract_size_unit=invalid")
		}
		if r.ContractSizeCurrency == nil {
			return fmt.Errorf("failed to validate open interest: contract_size_currency=null")
		}
		if strings.TrimSpace(*r.ContractSizeCurrency) == "" {
			return fmt.Errorf("failed to validate open interest: contract_size_currency=empty")
		}
	}
	return nil
}

func validNonNegativeOpenInterestNumber(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}
