import dispersals


def test_dispersal_logger_register_ledger():
    # Given a new DisperalsLogger
    logger = dispersals.DispersalsLogger()
    # Then no ledgers registered
    assert logger.ledger_names == []

    # When ledgers added
    logger.register_ledger(name="bank", ledger_transactions=dispersals.InMemoryTransactionLedger())
    logger.register_ledger(name="purchase_ledger", ledger_transactions=dispersals.InMemoryTransactionLedger())
    # Then bank and purchase_ledger in ledger_names
    assert logger.ledger_names == ["bank", "purchase_ledger"]


def test_dispersal_logger_undispersed_transactions_empty():
    # Given a Dispersals logger with a registered ledger
    logger = dispersals.DispersalsLogger()
    logger.register_ledger(name="bank", ledger_transactions=dispersals.InMemoryTransactionLedger())
    # When getting undispersed transactions
    # Then no transactions
    assert logger.undispersed_transactions(name="bank") == []


def test_dispersal_logger_undispersed_transactions():
    # Given a Dispersals logger with a registered ledger and a transactions ledger with transactions
    logger = dispersals.DispersalsLogger()
    ledger = dispersals.InMemoryTransactionLedger()
    class Transaction:
        def __init__(self, id):
            self.transaction_id = id
            return
    original_transactions = [Transaction(1), Transaction(2), Transaction(3)]
    ledger.transactions.extend(original_transactions)
    logger.register_ledger(name="bank", ledger_transactions=ledger)
    # When listing undispersed_transactions
    # Then undispersed equal to original transactions
    assert logger.undispersed_transactions(name="bank") == original_transactions


def test_dispersal_logger_log_dispersal():
    # Given a Dispersals Logger with a registered and a transactions ledger with transactions
    logger = dispersals.DispersalsLogger()
    ledger = dispersals.InMemoryTransactionLedger()
    class Transaction:
        def __init__(self, id):
            self.transaction_id = id
            return
    original_transactions = [Transaction(1), Transaction(2), Transaction(3)]
    ledger.transactions.extend(original_transactions)
    logger.register_ledger(name="bank", ledger_transactions=ledger)
    # When logging a sub set of transactions as dispersed
    logger.log_dispersal(name="bank", transactions=original_transactions[:2])
    # Then undispersed is difference between subset and original
    assert logger.undispersed_transactions(name="bank") == original_transactions[2:]

