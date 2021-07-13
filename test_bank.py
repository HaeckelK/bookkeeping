import pytest

import bank


@pytest.fixture
def raw_bank_transactions_clean():
    return [
        bank.RawBankTransaction(
            raw_id=1,
            bank_code="bank_code",
            transfer_type="transfer_type",
            description="description",
            amount=100,
            date="date",
            matched_account="matched_account",
            matched_type="matched_type",
            transaction_type="transaction_type",
        )
    ]


def test_in_memory_bank_transactions_init():
    bank.InMemoryBankLedgerTransactions()
    assert 1


def test_in_memory_bank_transactions_list_transactions_none():
    ledger = bank.InMemoryBankLedgerTransactions()
    assert ledger.list_transactions() == []


def test_in_memory_bank_transactions_add_transaction_empty(raw_bank_transactions_clean):
    # Given an empty bank ledger
    ledger = bank.InMemoryBankLedgerTransactions()
    # When adding transactions
    ledger.add_transactions(raw_bank_transactions_clean)
    # Then number of transactions added is number of raw transactions
    assert len(ledger.list_transactions()) == len(raw_bank_transactions_clean)
    # Then details converted as required
    assert ledger.list_transactions() == [
        bank.BankTransaction(
            raw_id=1,
            bank_code="bank_code",
            transfer_type="transfer_type",
            transaction_type="transaction_type",
            description="description",
            amount=100,
            date="date",
            matched_account="matched_account",
            matched_type="matched_type",
            transaction_id=0,
            batch_id=0,
            gl_jnl=False,
        )
    ]


def test_in_memory_bank_transactions_add_transaction_existing(raw_bank_transactions_clean):
    # Given a bank ledger with transactions
    ledger = bank.InMemoryBankLedgerTransactions()
    ledger.add_transactions(raw_bank_transactions_clean)
    # When adding transactions
    ledger.add_transactions(raw_bank_transactions_clean)
    # Then number of transactions added is number of raw transactions * 2
    assert len(ledger.list_transactions()) == len(raw_bank_transactions_clean) * 2


def test_in_memory_bank_transactions_transaction_id_unique(raw_bank_transactions_clean):
    # Given an empty bank ledger
    ledger = bank.InMemoryBankLedgerTransactions()
    # When adding transactions
    ledger.add_transactions(raw_bank_transactions_clean)
    ledger.add_transactions(raw_bank_transactions_clean)
    # Then transaction ids unique
    assert len(set([x.transaction_id for x in ledger.list_transactions()])) == len(ledger.list_transactions())


def test_in_memory_bank_transactions_transaction_id_ordered(raw_bank_transactions_clean):
    # Given an empty bank ledger
    ledger = bank.InMemoryBankLedgerTransactions()
    # When adding transactions
    ledger.add_transactions(raw_bank_transactions_clean)
    ledger.add_transactions(raw_bank_transactions_clean)
    # Then transaction ids ordered
    transaction_ids = [x.transaction_id for x in ledger.list_transactions()]
    assert transaction_ids == sorted(transaction_ids)


def test_in_memory_bank_transactions_batch_id(raw_bank_transactions_clean):
    # Given an empty bank ledger
    ledger = bank.InMemoryBankLedgerTransactions()
    # When adding transactions in batches
    ledger.add_transactions(raw_bank_transactions_clean * 2)
    # Then only one batch id
    assert len(set([x.batch_id for x in ledger.list_transactions()])) == 1
    # When adding another set of transactions
    ledger.add_transactions(raw_bank_transactions_clean)
    # Then two batch ids
    assert len(set([x.batch_id for x in ledger.list_transactions()])) == 2
