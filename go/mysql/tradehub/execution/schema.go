package execution

import (
	"context"
	"fmt"

	k4k3ruStorageAPI "github.com/k4k3ru-hub/storage/go/api"
)

// CreateTables creates the Trade Hub execution snapshot, leg, and onchain transaction tables.
//
// Parameters:
//   - ctx: Operation context.
//   - executor: SQL executor.
//
// Version:
//   - 2026-09-10: Added.
func (s *Store) CreateTables(ctx context.Context, executor k4k3ruStorageAPI.Executor) error {
	const operation = "failed to create trade hub execution tables"
	if err := s.validateOperation(ctx, executor, operation); err != nil {
		return err
	}
	queries := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			status TINYINT UNSIGNED NOT NULL,
			kind VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			request_snapshot JSON NOT NULL,
			conditions_snapshot JSON NULL,
			opportunity_snapshot JSON NULL,
			result_snapshot JSON NULL,
			prepared_at DATETIME(6) NOT NULL,
			expires_at DATETIME(6) NOT NULL,
			completed_at DATETIME(6) NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id),
			KEY idx_trade_hub_executions_status_expires_at (status, expires_at),
			KEY idx_trade_hub_executions_kind_created_at (kind, created_at)
		) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;`, s.executionTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id BIGINT UNSIGNED NOT NULL,
			execution_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			leg_index SMALLINT UNSIGNED NOT NULL,
			category TINYINT UNSIGNED NOT NULL,
			status TINYINT UNSIGNED NOT NULL,
			venue VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			PRIMARY KEY (id),
			UNIQUE KEY uk_trade_hub_execution_legs_execution_id_leg_index (execution_id, leg_index),
			KEY idx_trade_hub_execution_legs_status (status),
			CONSTRAINT fk_trade_hub_execution_legs_execution_id FOREIGN KEY (execution_id) REFERENCES %s (id) ON DELETE CASCADE ON UPDATE CASCADE
		) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;`, s.legTable, s.executionTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			execution_leg_id BIGINT UNSIGNED NOT NULL,
			chain_family VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			chain VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			network VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			signer VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			payload_digest VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
			transaction_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NULL,
			block_number VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin NULL,
			gas_used VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin NULL,
			fee_amount VARCHAR(78) CHARACTER SET ascii COLLATE ascii_bin NULL,
			fee_asset VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
			submission_started_at DATETIME(6) NULL,
			submitted_at DATETIME(6) NULL,
			confirmed_at DATETIME(6) NULL,
			created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
			PRIMARY KEY (execution_leg_id),
			UNIQUE KEY uk_trade_hub_execution_onchain_tx_chain_network_transaction_id (chain, network, transaction_id),
			KEY idx_trade_hub_execution_onchain_tx_signer_created_at (signer, created_at),
			CONSTRAINT fk_trade_hub_execution_onchain_tx_execution_leg_id FOREIGN KEY (execution_leg_id) REFERENCES %s (id) ON DELETE CASCADE ON UPDATE CASCADE
		) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4;`, s.onchainTransactionTable, s.legTable),
	}
	for _, query := range queries {
		if _, err := executor.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("%s: %w", operation, err)
		}
	}
	return nil
}
