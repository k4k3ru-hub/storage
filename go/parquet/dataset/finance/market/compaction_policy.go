package market

import (
	"cmp"
	"fmt"
	"math"
	"strings"
)

type bboCompactionPolicy struct{}

func (bboCompactionPolicy) Compare(left, right BBO) int {
	if value := left.EventTimestamp.Compare(right.EventTimestamp); value != 0 {
		return value
	}
	if value := left.ReceivedTimestamp.Compare(right.ReceivedTimestamp); value != 0 {
		return value
	}
	if value := cmp.Compare(left.BidPrice, right.BidPrice); value != 0 {
		return value
	}
	if value := cmp.Compare(left.BidQuantity, right.BidQuantity); value != 0 {
		return value
	}
	if value := cmp.Compare(left.AskPrice, right.AskPrice); value != 0 {
		return value
	}
	return cmp.Compare(left.AskQuantity, right.AskQuantity)
}

func (bboCompactionPolicy) DeduplicationKey(record BBO) (string, bool) {
	return fmt.Sprintf(
		"%d:%d:%016x:%016x:%016x:%016x",
		record.EventTimestamp.UnixMicro(),
		record.ReceivedTimestamp.UnixMicro(),
		math.Float64bits(record.BidPrice),
		math.Float64bits(record.BidQuantity),
		math.Float64bits(record.AskPrice),
		math.Float64bits(record.AskQuantity),
	), true
}

type tradeCompactionPolicy struct{}

func (tradeCompactionPolicy) Compare(left, right Trade) int {
	if value := left.EventTimestamp.Compare(right.EventTimestamp); value != 0 {
		return value
	}
	if value := cmp.Compare(left.TradeID, right.TradeID); value != 0 {
		return value
	}
	if value := left.ReceivedTimestamp.Compare(right.ReceivedTimestamp); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Side, right.Side); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Price, right.Price); value != 0 {
		return value
	}
	return cmp.Compare(left.Quantity, right.Quantity)
}

func (tradeCompactionPolicy) DeduplicationKey(record Trade) (string, bool) {
	tradeID := strings.TrimSpace(record.TradeID)
	return tradeID, tradeID != ""
}

type candleCompactionPolicy struct{}

func (candleCompactionPolicy) Compare(left, right Candle) int {
	if value := left.Timestamp.Compare(right.Timestamp); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Open, right.Open); value != 0 {
		return value
	}
	if value := cmp.Compare(left.High, right.High); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Low, right.Low); value != 0 {
		return value
	}
	if value := cmp.Compare(left.Close, right.Close); value != 0 {
		return value
	}
	return cmp.Compare(left.Volume, right.Volume)
}

func (candleCompactionPolicy) DeduplicationKey(record Candle) (string, bool) {
	return fmt.Sprintf("%d", record.Timestamp.UnixMicro()), true
}
