package oms

const perpetualEventColumns = eventColumns + ",network,trading_account,market_id,client_order_id,venue_order_id,venue_status,venue_status_time_ms,observed_at,fill_id,fill_identity_hash,fill_time_ms,start_position,direction,liquidity,closed_pnl,closed_pnl_asset_id,closed_pnl_decimals,protocol_version,protocol_data"

func perpetualEventArgs(r PerpetualEvent) []any {
	d := r.Perpetual
	return append(eventArgs(r.Event), d.Network, d.TradingAccount, d.MarketID, d.ClientOrderID, d.VenueOrderID, d.VenueStatus, d.VenueStatusTimeMS, d.ObservedAt, d.FillID, d.FillIdentityHash, d.FillTimeMS, d.StartPosition, d.Direction, d.Liquidity, d.ClosedPnL, d.ClosedPnLAssetID, d.ClosedPnLDecimals, d.ProtocolVersion, d.ProtocolData)
}
func scanPerpetualEvent(row scanner) (*PerpetualEvent, error) {
	var v Event
	var d PerpetualEvidence
	if err := row.Scan(&v.ID, &v.OrderID, &v.ExecutionRecordID, &v.RequestedQuantity, &v.Sequence, &v.EventType, &v.SubmissionEventID, &v.ReferenceEventID, &v.RecordKey, &v.ExecutionSystem, &v.ExecutionID, &v.Venue, &v.Quantity, &v.CounterQuantity, &v.QuantityAssetID, &v.CounterAssetID, &v.QuantityDecimals, &v.CounterDecimals, &v.OrderQuantity, &v.OrderCounterQuantity, &v.Price, &v.FeesComplete, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt, &d.Network, &d.TradingAccount, &d.MarketID, &d.ClientOrderID, &d.VenueOrderID, &d.VenueStatus, &d.VenueStatusTimeMS, &d.ObservedAt, &d.FillID, &d.FillIdentityHash, &d.FillTimeMS, &d.StartPosition, &d.Direction, &d.Liquidity, &d.ClosedPnL, &d.ClosedPnLAssetID, &d.ClosedPnLDecimals, &d.ProtocolVersion, (*[]byte)(&d.ProtocolData)); err != nil {
		return nil, err
	}
	return &PerpetualEvent{Event: v, Perpetual: d}, nil
}
