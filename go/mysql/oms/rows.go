package oms

import "strings"

const orderColumns = "id,account_id,parent_order_id,account_ref,asset_class,domain,venue,symbol,side,order_type,status,quantity,filled_quantity,limit_price,take_profit_type,take_profit_value,stop_loss_type,stop_loss_value,specification_version,specification,last_execution_sequence,idempotency_key,expires_at,completed_at,created_at,updated_at"

func scanOrder(row scanner) (*Order, error) {
	var v Order
	if err := row.Scan(&v.ID, &v.AccountID, &v.ParentOrderID, &v.AccountRef, &v.AssetClass, &v.Domain, &v.Venue, &v.Symbol, &v.Side, &v.OrderType, &v.Status, &v.Quantity, &v.FilledQuantity, &v.LimitPrice, &v.TakeProfitType, &v.TakeProfitValue, &v.StopLossType, &v.StopLossValue, &v.SpecificationVersion, &v.Specification, &v.LastExecutionSequence, &v.IdempotencyKey, &v.ExpiresAt, &v.CompletedAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func orderArgs(v Order) []any {
	return []any{v.ID, v.AccountID, v.ParentOrderID, v.AccountRef, v.AssetClass, v.Domain, v.Venue, v.Symbol, v.Side, v.OrderType, v.Status, v.Quantity, v.FilledQuantity, v.LimitPrice, v.TakeProfitType, v.TakeProfitValue, v.StopLossType, v.StopLossValue, v.SpecificationVersion, v.Specification, v.LastExecutionSequence, v.IdempotencyKey, v.ExpiresAt, v.CompletedAt, v.CreatedAt, v.UpdatedAt}
}

const executionColumns = "id,order_id,sequence,exec_type,submission_record_id,reference_record_id,record_key,execution_system,execution_id,venue,quantity,counter_quantity,quantity_asset_id,counter_asset_id,quantity_decimals,counter_decimals,order_quantity,price,fees_complete,source_version,occurred_at,recorded_at"

func scanExecution(row scanner) (*Execution, error) {
	var v Execution
	if err := row.Scan(&v.ID, &v.OrderID, &v.Sequence, &v.ExecType, &v.SubmissionRecordID, &v.ReferenceRecordID, &v.RecordKey, &v.ExecutionSystem, &v.ExecutionID, &v.Venue, &v.Quantity, &v.CounterQuantity, &v.QuantityAssetID, &v.CounterAssetID, &v.QuantityDecimals, &v.CounterDecimals, &v.OrderQuantity, &v.Price, &v.FeesComplete, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func executionArgs(v Execution) []any {
	return []any{v.ID, v.OrderID, v.Sequence, v.ExecType, v.SubmissionRecordID, v.ReferenceRecordID, v.RecordKey, v.ExecutionSystem, v.ExecutionID, v.Venue, v.Quantity, v.CounterQuantity, v.QuantityAssetID, v.CounterAssetID, v.QuantityDecimals, v.CounterDecimals, v.OrderQuantity, v.Price, v.FeesComplete, v.SourceVersion, v.OccurredAt, v.RecordedAt}
}

const onchainDetailColumns = "execution_record_id,order_id,exec_type,chain_family,chain,network,tx_id,ledger_unit,ledger_sequence,ledger_id,tx_position,event_position,emitter_id,pool_id,signer_id,recipient_id,payload_digest,payload_encoding,tx_payload,finality_level,protocol_version,protocol_data"

func scanOnchainDetail(row scanner) (*OnchainDetail, error) {
	var v OnchainDetail
	if err := row.Scan(&v.ExecutionRecordID, &v.OrderID, &v.ExecType, &v.ChainFamily, &v.Chain, &v.Network, &v.TxID, &v.LedgerUnit, &v.LedgerSequence, &v.LedgerID, &v.TxPosition, &v.EventPosition, &v.EmitterID, &v.PoolID, &v.SignerID, &v.RecipientID, &v.PayloadDigest, &v.PayloadEncoding, &v.TxPayload, &v.FinalityLevel, &v.ProtocolVersion, (*[]byte)(&v.ProtocolData)); err != nil {
		return nil, err
	}
	return &v, nil
}
func onchainDetailArgs(v OnchainDetail) []any {
	return []any{v.ExecutionRecordID, v.OrderID, v.ExecType, v.ChainFamily, v.Chain, v.Network, v.TxID, v.LedgerUnit, v.LedgerSequence, v.LedgerID, v.TxPosition, v.EventPosition, v.EmitterID, v.PoolID, v.SignerID, v.RecipientID, v.PayloadDigest, v.PayloadEncoding, v.TxPayload, v.FinalityLevel, v.ProtocolVersion, v.ProtocolData}
}

const executionFeeColumns = "id,order_id,execution_record_id,adjustment_of_fee_id,record_key,fee_type,accounting_treatment,asset_namespace,asset_chain,asset_network,asset_id,asset_decimals,amount,source_reference,source_version,occurred_at,recorded_at"

func scanExecutionFee(row scanner) (*ExecutionFee, error) {
	var v ExecutionFee
	if err := row.Scan(&v.ID, &v.OrderID, &v.ExecutionRecordID, &v.AdjustmentOfFeeID, &v.RecordKey, &v.FeeType, &v.AccountingTreatment, &v.AssetNamespace, &v.AssetChain, &v.AssetNetwork, &v.AssetID, &v.AssetDecimals, &v.Amount, &v.SourceReference, &v.SourceVersion, &v.OccurredAt, &v.RecordedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func executionFeeArgs(v ExecutionFee) []any {
	return []any{v.ID, v.OrderID, v.ExecutionRecordID, v.AdjustmentOfFeeID, v.RecordKey, v.FeeType, v.AccountingTreatment, v.AssetNamespace, v.AssetChain, v.AssetNetwork, v.AssetID, v.AssetDecimals, v.Amount, v.SourceReference, v.SourceVersion, v.OccurredAt, v.RecordedAt}
}

func insertSQL(table, columns string) string {
	return "INSERT INTO " + quoted(table) + " (" + columns + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", strings.Count(columns, ",")+1), ",") + ")"
}
