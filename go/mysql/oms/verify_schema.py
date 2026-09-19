"""Verify reviewed OMS DDL in a disposable, network-isolated MySQL 8.4 container."""

from decimal import Decimal
from pathlib import Path
import subprocess
import time
import uuid


def main():
    name = "oms-schema-check-" + uuid.uuid4().hex[:12]
    created = False
    checks = 0

    def sql(statement, expected_error=None):
        nonlocal checks
        result = subprocess.run(
            ["docker", "exec", "-i", name, "mysql", "--user=root", "--batch",
             "--skip-column-names", "oms_review"],
            input=statement, text=True, capture_output=True, timeout=30,
        )
        if expected_error is not None:
            assert result.returncode != 0 and f"ERROR {expected_error} " in result.stderr, result.stderr
            checks += 1
        elif result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def check(statement, expected):
        nonlocal checks
        actual = sql(statement)
        assert actual == expected, (statement, actual, expected)
        checks += 1

    def order(order_id, account_id=1, key=None, parent="NULL"):
        key = key or f"order-{order_id}"
        return f"""INSERT INTO oms_orders
            (id,account_id,parent_order_id,account_ref,asset_class,domain,venue,symbol,
             side,order_type,status,quantity,idempotency_key)
            VALUES ({order_id},{account_id},{parent},'wallet','crypto','onchain-amm-pool',
             'uniswap-v3','USDC/ABC','exchange','limit','pending','100','{key}');"""

    def execution(record_id, key, kind="filled", purpose="trade", quantity="'30'",
                  counter="'3'", order_id=1, attempt=2, expiry="NULL"):
        return f"""INSERT INTO oms_order_executions
            (id,order_id,attempt_number,exec_type,purpose,status,record_key,
             execution_system,execution_id,quantity,counter_quantity,occurred_at,expires_at)
            VALUES ({record_id},{order_id},{attempt},'{kind}','{purpose}','succeeded','{key}',
             'tradehub','exec_shared',{quantity},{counter},UTC_TIMESTAMP(6),{expiry});"""

    try:
        subprocess.run(
            ["docker", "run", "--detach", "--name", name, "--network", "none",
             "--tmpfs", "/var/lib/mysql", "-e", "MYSQL_ALLOW_EMPTY_PASSWORD=yes",
             "-e", "MYSQL_DATABASE=oms_review", "mysql:8.4"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        created = True
        deadline = time.monotonic() + 50
        while True:
            ping = subprocess.run(
                ["docker", "exec", name, "mysql", "--user=root", "oms_review", "-e", "SELECT 1"],
                capture_output=True, timeout=5,
            )
            if ping.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("temporary MySQL startup timed out")
            time.sleep(0.5)

        sql(Path(__file__).with_name("schema.proposed.sql").read_text())
        check("SHOW TABLES;", "oms_order_executions\noms_order_onchain_amm_pool_swaps\noms_orders")
        sql(order(1))
        sql(order(2, parent="1"))
        sql("UPDATE oms_orders SET quantity=NULL,take_profit_type='return_bps',take_profit_value='2000',stop_loss_type='return_bps',stop_loss_value='-1000' WHERE id=2;")
        check("SELECT parent_order_id, quantity IS NULL FROM oms_orders WHERE id=2;", "1\t1")
        sql(order(3, key="order-1"), 1062)
        sql(order(3, account_id=2, key="order-1"))
        sql(order(4, parent="999"), 1452)
        for expression in [
            "take_profit_type='price'", "take_profit_value='1'",
            "take_profit_type='invalid',take_profit_value='1'",
            "stop_loss_type='price'", "stop_loss_value='1'",
            "stop_loss_type='invalid',stop_loss_value='1'",
        ]:
            sql(f"UPDATE oms_orders SET {expression} WHERE id=1;", 3819)
        sql("UPDATE oms_orders SET limit_price='0.2',take_profit_type='price',take_profit_value='0.3',stop_loss_type='price',stop_loss_value='0.1' WHERE id=1;")
        swap = """INSERT INTO oms_order_onchain_amm_pool_swaps
            (order_id,chain_family,chain,network,pool_id,token_in_id,token_out_id,
             token_in_decimals,token_out_decimals,swap_kind,signer,recipient,
             maximum_slippage_bps,execution_ttl_ms)
            VALUES (1,'evm','base','sepolia','pool','token-in','token-out',6,18,
             'exact-input','signer','recipient',100,60000);"""
        sql(swap)
        sql(swap, 1062)
        sql(swap.replace("VALUES (1,", "VALUES (999,"), 1452)
        for expression in ["swap_kind='bad'", "maximum_slippage_bps=10001", "execution_ttl_ms=0"]:
            sql(f"UPDATE oms_order_onchain_amm_pool_swaps SET {expression} WHERE order_id=1;", 3819)
        sql(execution(10, "prepare", "prepared", "approval", "NULL", "NULL", attempt=1, expiry="UTC_TIMESTAMP(6)"))
        sql(execution(11, "submit", "submitted", "approval", "NULL", "NULL", attempt=1))
        sql(execution(20, "fill-a", order_id=999), 1452)
        sql(execution(20, "fill-a", attempt=0), 3819)
        sql(execution(20, "fill-a", purpose="approval"), 3819)
        sql(execution(20, "fill-a", quantity="NULL"), 3819)
        sql(execution(20, "fill-a", counter="NULL"), 3819)
        sql(execution(20, "fill-a", kind="submitted"), 3819)
        sql(execution(20, "fill-a", expiry="UTC_TIMESTAMP(6)"), 3819)

        # Explicit snapshot update, not a DB trigger: demonstrates atomic storage only.
        sql("START TRANSACTION; SELECT id FROM oms_orders WHERE id=1 FOR UPDATE;" +
            execution(20, "fill-a") + execution(21, "fill-b", quantity="'20'", counter="'2'") +
            "UPDATE oms_orders SET filled_quantity='50',status='partially_filled' WHERE id=1; COMMIT;")
        check("SELECT COUNT(*) FROM oms_order_executions WHERE execution_id='exec_shared';", "4")
        check("SELECT filled_quantity,status FROM oms_orders WHERE id=1;", "50\tpartially_filled")
        amounts = sql("SELECT quantity FROM oms_order_executions WHERE order_id=1 AND exec_type='filled' AND status='succeeded';")
        assert sum(map(Decimal, amounts.splitlines())) == Decimal("50")
        checks += 1
        sql(execution(22, "fill-a"), 1062)
        check("SELECT filled_quantity FROM oms_orders WHERE id=1;", "50")
        sql("START TRANSACTION; UPDATE oms_order_executions SET status='reversed' WHERE id=20; UPDATE oms_orders SET filled_quantity='20' WHERE id=1; COMMIT;")
        check("SELECT filled_quantity FROM oms_orders WHERE id=1;", "20")
        check("SELECT quantity FROM oms_order_executions WHERE exec_type='filled' AND status='succeeded';", "20")
        sql("START TRANSACTION;" + execution(22, "fill-c", quantity="'5'") +
            "UPDATE oms_orders SET filled_quantity='25' WHERE id=1; ROLLBACK;")
        check("SELECT COUNT(*) FROM oms_order_executions WHERE id=22;", "0")
        check("SELECT filled_quantity FROM oms_orders WHERE id=1;", "20")
        precise = "1." + "0" * 100 + "1"
        sql(f"UPDATE oms_orders SET quantity='{precise}' WHERE id=3;")
        check("SELECT quantity FROM oms_orders WHERE id=3;", precise)
        sql("DELETE FROM oms_orders WHERE id=1;", 1451)
        print(f"PASS: {checks} checks; DDL, foreign keys, constraints, partial fills, duplicate delivery, reversal, rollback, decimal preservation.")
    finally:
        if created:
            subprocess.run(["docker", "rm", "--force", name], check=True, capture_output=True, timeout=30)
            print("Temporary MySQL removed; existing databases unchanged.")


if __name__ == "__main__":
    main()
