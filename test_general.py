from general import GLJournal, GLJournalLine
import general


def test_create_opposite_journal():
    # Given a journal with multiple lines with non zero amount
    journal = GLJournal(
        jnl_type="gnl",
        lines=[
            GLJournalLine(nominal="abc", description="description for abc", amount=123, transaction_date="01/01/1990"),
            GLJournalLine(nominal="def", description="description for def", amount=500, transaction_date="01/01/1990"),
            GLJournalLine(nominal="ghi", description="description for ghi", amount=-623, transaction_date="01/01/1990"),
        ],
    )
    # When creating an inverse journal
    new_journal = general.create_opposite_journal(journal)
    # Then both journals have same number of lines
    assert len(journal.lines) == len(new_journal.lines)
    # Then both journals have the same type
    assert journal.jnl_type == new_journal.jnl_type
    # Then in same order
    for i in range(len(journal.lines)):
        line = journal.lines[i]
        new_line = new_journal.lines[i]
        # Then both have same nominal
        assert line.nominal == new_line.nominal
        # Then description same
        assert line.description == new_line.description
        # Then have opposite amounts
        assert line.amount == -new_line.amount
        # Then have same transaction_date
        assert line.transaction_date == new_line.transaction_date


def test_add_reversing_journal():
    # Given a GeneralLedger with no transactions
    ledger = general.GeneralLedger(ledger=general.GeneralLedgerTransactions(), chart_of_accounts=None)


# TODO having to use this as date to period conversion is currently based on a pandas object to int conversion.
# Function requires object to have .month attribute
class DummyDate:
    def __init__(self, year: int, month: int, day: int):
        self.year = year
        self.month = month
        self.day = day
        return

    def __str__(self) -> str:
        return f"{self.day}/{self.month}/{self.year}"


def test_general_ledger_add_journal():
    # Given a GeneralLedger with no transactions
    ledger = general.GeneralLedger(ledger=general.GeneralLedgerTransactions(), chart_of_accounts=None)
    # When adding a journal
    journal = GLJournal(
        jnl_type="gnl",
        lines=[
            GLJournalLine(nominal="abc", description="description for abc", amount=123, transaction_date="01/01/2021"),
            GLJournalLine(nominal="def", description="description for def", amount=-123, transaction_date="01/01/2021"),
        ],
    )
    ledger.add_journal(journal)
    # Then GL balances
    assert ledger.ledger.balance == 0
    # Then num transactions == num journal lines
    assert len(ledger.ledger.list_transactions()) == len(journal.lines)
    # Then journal lines correctly represented
    for line in journal.lines:
        assert ledger.ledger.balances[line.nominal] == line.amount
    print(ledger.ledger.list_transactions())
    assert ledger.ledger.list_transactions() == [
        general.GeneralLedgerTransaction(
            transaction_id=0,
            jnl_id=0,
            nominal="abc",
            jnl_type="gnl",
            amount=123,
            description="description for abc",
            transaction_date=DummyDate(2021, 1, 1),
            period=-1,
        ),
        general.GeneralLedgerTransaction(
            transaction_id=1,
            jnl_id=0,
            nominal="def",
            jnl_type="gnl",
            amount=-123,
            description="description for def",
            transaction_date=DummyDate(2021, 1, 1),
            period=-1,
        ),
    ]
