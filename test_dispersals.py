import dispersals


def test_dispersal_logger_register_ledger():
    # Given a new DisperalsLogger
    logger = dispersals.DispersalsLogger()
    # Then no ledgers registered
    assert logger.ledger_names == []

    # When ledgers added
    logger.register_ledger(name="bank", ledger_transactions=dispersals.TransactionsLedger())
    logger.register_ledger(name="purchase_ledger", ledger_transactions=dispersals.TransactionsLedger())
    # Then bank and purchase_ledger in ledger_names
    assert logger.ledger_names == ["bank", "purchase_ledger"]


def test_dispersal_logger_undispersed_transactions_empty():
    # Given a Dispersals logger with a registered ledger
    logger = dispersals.DispersalsLogger()
    logger.register_ledger(name="bank", ledger_transactions=dispersals.TransactionsLedger())
    # When getting undispersed transactions
    # Then no transactions
    assert logger.undispersed_transactions(name="bank") == []
