"""Verify OMS source and PnL DDL and Store constraints in a disposable MySQL."""

from verify_store import main

if __name__ == "__main__":
    main("TestSchemaParity|TestMySQLOnchain|TestMySQLRollback|TestMySQLPnL")
