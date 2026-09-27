package oms

import "strings"

const orderColumns = "id,account_id,parent_order_id,account_ref,asset_class,domain,venue,symbol,side,order_type,status,quantity,filled_quantity,filled_counter_quantity,limit_price,take_profit_type,take_profit_value,stop_loss_type,stop_loss_value,specification_version,specification,last_event_sequence,idempotency_key,expires_at,completed_at,created_at,updated_at"

func scanOrder(row scanner) (*Order, error) {
	var v Order
	if err := row.Scan(&v.ID, &v.AccountID, &v.ParentOrderID, &v.AccountRef, &v.AssetClass, &v.Domain, &v.Venue, &v.Symbol, &v.Side, &v.OrderType, &v.Status, &v.Quantity, &v.FilledQuantity, &v.FilledCounterQuantity, &v.LimitPrice, &v.TakeProfitType, &v.TakeProfitValue, &v.StopLossType, &v.StopLossValue, &v.SpecificationVersion, &v.Specification, &v.LastEventSequence, &v.IdempotencyKey, &v.ExpiresAt, &v.CompletedAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func orderArgs(v Order) []any {
	return []any{v.ID, v.AccountID, v.ParentOrderID, v.AccountRef, v.AssetClass, v.Domain, v.Venue, v.Symbol, v.Side, v.OrderType, v.Status, v.Quantity, v.FilledQuantity, v.FilledCounterQuantity, v.LimitPrice, v.TakeProfitType, v.TakeProfitValue, v.StopLossType, v.StopLossValue, v.SpecificationVersion, v.Specification, v.LastEventSequence, v.IdempotencyKey, v.ExpiresAt, v.CompletedAt, v.CreatedAt, v.UpdatedAt}
}

const eventColumns = "id,order_id,execution_record_id,requested_quantity,sequence,event_type,submission_event_id,reference_event_id,record_key,execution_system,execution_id,venue,quantity,counter_quantity,quantity_asset_id,counter_asset_id,quantity_decimals,counter_decimals,order_quantity,order_counter_quantity,price,fees_complete,source_version,occurred_at,recorded_at"

func scanEvent(row scanner) (*Event, error) {
	var v Event
	if err := row.Scan(&v.ID, &v.OrderID, &v.ExecutionRecordID, &v.RequestedQuantity, &v.Sequence, &v.EventType, &v.SubmissionEventID, &v.ReferenceEventID, &v.RecordKey, &v.ExecutionSystem, &v.ExecutionID, &v.Venue, &v.Quantity, &v.CounterQuantity, &v.QuantityAssetID, &v.CounterAssetID, &v.QuantityDecimals, &v.CounterDecimals, &v.OrderQuantity, &v.OrderCounterQuantity, &v.Price, &v.FeesComplete, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func eventArgs(v Event) []any {
	return []any{v.ID, v.OrderID, v.ExecutionRecordID, v.RequestedQuantity, v.Sequence, v.EventType, v.SubmissionEventID, v.ReferenceEventID, v.RecordKey, v.ExecutionSystem, v.ExecutionID, v.Venue, v.Quantity, v.CounterQuantity, v.QuantityAssetID, v.CounterAssetID, v.QuantityDecimals, v.CounterDecimals, v.OrderQuantity, v.OrderCounterQuantity, v.Price, v.FeesComplete, v.SourceVersion, v.OccurredAt, v.RecordedAt}
}

const onchainEvidenceColumns = "id,order_id,event_type,chain_family,chain,network,tx_id,ledger_unit,ledger_sequence,ledger_id,tx_position,event_position,emitter_id,pool_id,signer_id,recipient_id,payload_digest,payload_encoding,tx_payload,finality_level,protocol_version,protocol_data"

func scanOnchainEvidence(row scanner) (*OnchainEvidence, error) {
	var v OnchainEvidence
	if err := row.Scan(&v.EventID, &v.OrderID, &v.EventType, &v.ChainFamily, &v.Chain, &v.Network, &v.TxID, &v.LedgerUnit, &v.LedgerSequence, &v.LedgerID, &v.TxPosition, &v.EventPosition, &v.EmitterID, &v.PoolID, &v.SignerID, &v.RecipientID, &v.PayloadDigest, &v.PayloadEncoding, &v.TxPayload, &v.FinalityLevel, &v.ProtocolVersion, (*[]byte)(&v.ProtocolData)); err != nil {
		return nil, err
	}
	return &v, nil
}
func onchainEvidenceArgs(v OnchainEvidence) []any {
	return []any{v.EventID, v.OrderID, v.EventType, v.ChainFamily, v.Chain, v.Network, v.TxID, v.LedgerUnit, v.LedgerSequence, v.LedgerID, v.TxPosition, v.EventPosition, v.EmitterID, v.PoolID, v.SignerID, v.RecipientID, v.PayloadDigest, v.PayloadEncoding, v.TxPayload, v.FinalityLevel, v.ProtocolVersion, v.ProtocolData}
}

const executionFeeColumns = "id,order_id,execution_record_id,event_id,adjustment_of_fee_id,record_key,fee_type,accounting_treatment,asset_namespace,asset_chain,asset_network,asset_id,asset_decimals,amount,source_reference,source_version,occurred_at,recorded_at"

func scanExecutionFee(row scanner) (*ExecutionFee, error) {
	var v ExecutionFee
	if err := row.Scan(&v.ID, &v.OrderID, &v.ExecutionRecordID, &v.EventID, &v.AdjustmentOfFeeID, &v.RecordKey, &v.FeeType, &v.AccountingTreatment, &v.AssetNamespace, &v.AssetChain, &v.AssetNetwork, &v.AssetID, &v.AssetDecimals, &v.Amount, &v.SourceReference, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func executionFeeArgs(v ExecutionFee) []any {
	return []any{v.ID, v.OrderID, v.ExecutionRecordID, v.EventID, v.AdjustmentOfFeeID, v.RecordKey, v.FeeType, v.AccountingTreatment, v.AssetNamespace, v.AssetChain, v.AssetNetwork, v.AssetID, v.AssetDecimals, v.Amount, v.SourceReference, v.SourceVersion, v.OccurredAt, v.RecordedAt}
}

func insertSQL(table, columns string) string {
	return "INSERT INTO " + quoted(table) + " (" + columns + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", strings.Count(columns, ",")+1), ",") + ")"
}

const executionColumns = "id,order_id,execution_system,execution_id,event_family,venue,status,quantity,filled_quantity,filled_counter_quantity,fees_complete,last_event_sequence,completed_at,created_at,updated_at"

func scanExecution(row scanner) (*Execution, error) {
	var e Execution
	if err := row.Scan(&e.ID, &e.OrderID, &e.ExecutionSystem, &e.ExecutionID, &e.EventFamily, &e.Venue, &e.Status, &e.Quantity, &e.FilledQuantity, &e.FilledCounterQuantity, &e.FeesComplete, &e.LastEventSequence, &e.CompletedAt, &e.CreatedAt, &e.UpdatedAt); err != nil {
		return nil, err
	}
	return &e, nil
}
func executionArgs(e Execution) []any {
	return []any{e.ID, e.OrderID, e.ExecutionSystem, e.ExecutionID, e.EventFamily, e.Venue, e.Status, e.Quantity, e.FilledQuantity, e.FilledCounterQuantity, e.FeesComplete, e.LastEventSequence, e.CompletedAt, e.CreatedAt, e.UpdatedAt}
}

var onchainEventColumns = eventColumns + "," + strings.Join(strings.Split(onchainEvidenceColumns, ",")[3:], ",")

func onchainEventArgs(r OnchainEvent) []any {
	return append(eventArgs(r.Event), onchainEvidenceArgs(*r.Onchain)[3:]...)
}
func scanOnchainEvent(row scanner) (*OnchainEvent, error) {
	var v Event
	var d OnchainEvidence
	if err := row.Scan(&v.ID, &v.OrderID, &v.ExecutionRecordID, &v.RequestedQuantity, &v.Sequence, &v.EventType, &v.SubmissionEventID, &v.ReferenceEventID, &v.RecordKey, &v.ExecutionSystem, &v.ExecutionID, &v.Venue, &v.Quantity, &v.CounterQuantity, &v.QuantityAssetID, &v.CounterAssetID, &v.QuantityDecimals, &v.CounterDecimals, &v.OrderQuantity, &v.OrderCounterQuantity, &v.Price, &v.FeesComplete, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt, &d.ChainFamily, &d.Chain, &d.Network, &d.TxID, &d.LedgerUnit, &d.LedgerSequence, &d.LedgerID, &d.TxPosition, &d.EventPosition, &d.EmitterID, &d.PoolID, &d.SignerID, &d.RecipientID, &d.PayloadDigest, &d.PayloadEncoding, &d.TxPayload, &d.FinalityLevel, &d.ProtocolVersion, (*[]byte)(&d.ProtocolData)); err != nil {
		return nil, err
	}
	d.EventID = v.ID
	d.OrderID = v.OrderID
	d.EventType = v.EventType
	return &OnchainEvent{Event: v, Onchain: &d}, nil
}
