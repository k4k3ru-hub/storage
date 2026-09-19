package oms

const orderColumns = "id,account_id,parent_order_id,account_ref,asset_class,domain,venue,symbol,side,order_type,status,quantity,filled_quantity,limit_price,take_profit_type,take_profit_value,stop_loss_type,stop_loss_value,idempotency_key,expires_at,completed_at,created_at,updated_at"
const orderPlaceholders = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"

func scanOrder(row scanner) (*Order, error) {
	var v Order
	if err := row.Scan(&v.ID, &v.AccountID, &v.ParentOrderID, &v.AccountRef, &v.AssetClass, &v.Domain, &v.Venue, &v.Symbol, &v.Side, &v.OrderType, &v.Status, &v.Quantity, &v.FilledQuantity, &v.LimitPrice, &v.TakeProfitType, &v.TakeProfitValue, &v.StopLossType, &v.StopLossValue, &v.IdempotencyKey, &v.ExpiresAt, &v.CompletedAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func orderArgs(v Order) []any {
	return []any{v.ID, v.AccountID, v.ParentOrderID, v.AccountRef, v.AssetClass, v.Domain, v.Venue, v.Symbol, v.Side, v.OrderType, v.Status, v.Quantity, v.FilledQuantity, v.LimitPrice, v.TakeProfitType, v.TakeProfitValue, v.StopLossType, v.StopLossValue, v.IdempotencyKey, v.ExpiresAt, v.CompletedAt, v.CreatedAt, v.UpdatedAt}
}

const swapColumns = "order_id,chain_family,chain,network,pool_id,token_in_id,token_out_id,token_in_decimals,token_out_decimals,swap_kind,signer,recipient,maximum_slippage_bps,execution_ttl_ms,created_at,updated_at"
const swapPlaceholders = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"

func scanSwap(row scanner) (*OnchainAMMPoolSwap, error) {
	var v OnchainAMMPoolSwap
	if err := row.Scan(&v.OrderID, &v.ChainFamily, &v.Chain, &v.Network, &v.PoolID, &v.TokenInID, &v.TokenOutID, &v.TokenInDecimals, &v.TokenOutDecimals, &v.SwapKind, &v.Signer, &v.Recipient, &v.MaximumSlippageBPS, &v.ExecutionTTLMS, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func swapArgs(v OnchainAMMPoolSwap) []any {
	return []any{v.OrderID, v.ChainFamily, v.Chain, v.Network, v.PoolID, v.TokenInID, v.TokenOutID, v.TokenInDecimals, v.TokenOutDecimals, v.SwapKind, v.Signer, v.Recipient, v.MaximumSlippageBPS, v.ExecutionTTLMS, v.CreatedAt, v.UpdatedAt}
}

const executionColumns = "id,order_id,attempt_number,exec_type,purpose,status,record_key,execution_system,execution_id,quantity,counter_quantity,source_version,occurred_at,expires_at,created_at,updated_at"
const executionPlaceholders = "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"

func scanExecution(row scanner) (*Execution, error) {
	var v Execution
	if err := row.Scan(&v.ID, &v.OrderID, &v.AttemptNumber, &v.ExecType, &v.Purpose, &v.Status, &v.RecordKey, &v.ExecutionSystem, &v.ExecutionID, &v.Quantity, &v.CounterQuantity, &v.SourceVersion, &v.OccurredAt, &v.ExpiresAt, &v.CreatedAt, &v.UpdatedAt); err != nil {
		return nil, err
	}
	return &v, nil
}
func executionArgs(v Execution) []any {
	return []any{v.ID, v.OrderID, v.AttemptNumber, v.ExecType, v.Purpose, v.Status, v.RecordKey, v.ExecutionSystem, v.ExecutionID, v.Quantity, v.CounterQuantity, v.SourceVersion, v.OccurredAt, v.ExpiresAt, v.CreatedAt, v.UpdatedAt}
}
