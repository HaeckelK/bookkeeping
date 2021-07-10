import datetime

from pandas import Timestamp

from general import GLJournal, GLJournalLine, GeneralLedger
import general


def test_create_opposite_journal():
    # Given a journal with multiple lines with non zero amount
    journal = GLJournal(
        jnl_type="gnl",
        transaction_date=datetime.datetime(2000, 1, 1),
        lines=[
            GLJournalLine(
                nominal="abc",
                description="description for abc",
                amount=123,
                transaction_date=datetime.datetime(1990, 1, 1),
            ),
            GLJournalLine(
                nominal="def",
                description="description for def",
                amount=500,
                transaction_date=datetime.datetime(1990, 1, 1),
            ),
            GLJournalLine(
                nominal="ghi",
                description="description for ghi",
                amount=-623,
                transaction_date=datetime.datetime(1990, 1, 1),
            ),
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


# TODO test transaction_ids returned
def test_general_ledger_add_journal():
    # Given a GeneralLedger with no transactions
    ledger = general.GeneralLedger(ledger=general.GeneralLedgerTransactions(), chart_of_accounts=None)
    # When adding a journal
    journal = GLJournal(
        jnl_type="gnl",
        transaction_date=datetime.datetime(2021, 1, 1),
        lines=[
            GLJournalLine(
                nominal="abc",
                description="description for abc",
                amount=123,
                transaction_date=datetime.datetime(2021, 1, 1),
            ),
            GLJournalLine(
                nominal="def",
                description="description for def",
                amount=-123,
                transaction_date=datetime.datetime(2021, 1, 1),
            ),
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
    assert ledger.ledger.list_transactions() == [
        general.GeneralLedgerTransaction(
            transaction_id=0,
            jnl_id=0,
            nominal="abc",
            jnl_type="gnl",
            amount=123,
            description="description for abc",
            transaction_date=Timestamp("2021-01-01 00:00:00"),
            period=1,
        ),
        general.GeneralLedgerTransaction(
            transaction_id=1,
            jnl_id=0,
            nominal="def",
            jnl_type="gnl",
            amount=-123,
            description="description for def",
            transaction_date=Timestamp("2021-01-01 00:00:00"),
            period=1,
        ),
    ]


# TODO test transaction_ids returned
def test_general_ledger_add_journal_reversing():
    # Given a GeneralLedger with no transactions
    ledger = general.GeneralLedger(ledger=general.GeneralLedgerTransactions(), chart_of_accounts=None)
    # When adding a journal marked as gnl_rev
    journal = GLJournal(
        jnl_type="gnl_rev",
        transaction_date=datetime.datetime(2021, 1, 1),
        lines=[
            GLJournalLine(
                nominal="abc",
                description="description for abc",
                amount=123,
                transaction_date=datetime.datetime(2021, 1, 1),
            ),
            GLJournalLine(
                nominal="def",
                description="description for def",
                amount=-123,
                transaction_date=datetime.datetime(2021, 1, 1),
            ),
        ],
    )
    ledger.add_journal(journal)
    # Then GL balances
    assert ledger.ledger.balance == 0
    # Then num transactions == num journal lines * 2
    assert len(ledger.ledger.list_transactions()) == len(journal.lines) * 2
    # Then journal lines correctly represented
    assert ledger.ledger.list_transactions() == [
        general.GeneralLedgerTransaction(
            transaction_id=0,
            jnl_id=0,
            nominal="abc",
            jnl_type="gnl_rev",
            amount=123,
            description="description for abc",
            transaction_date=Timestamp("2021-01-01 00:00:00"),
            period=1,
        ),
        general.GeneralLedgerTransaction(
            transaction_id=1,
            jnl_id=0,
            nominal="def",
            jnl_type="gnl_rev",
            amount=-123,
            description="description for def",
            transaction_date=Timestamp("2021-01-01 00:00:00"),
            period=1,
        ),
        general.GeneralLedgerTransaction(
            transaction_id=2,
            jnl_id=1,
            nominal="abc",
            jnl_type="gnl_rev",
            amount=-123,
            description="description for abc",
            transaction_date=Timestamp("2021-02-01 00:00:00"),
            period=2,
        ),
        general.GeneralLedgerTransaction(
            transaction_id=3,
            jnl_id=1,
            nominal="def",
            jnl_type="gnl_rev",
            amount=123,
            description="description for def",
            transaction_date=Timestamp("2021-02-01 00:00:00"),
            period=2,
        ),
    ]


# TODO balance at start or end
# TODO start or end of period
def test_create_prepayment_journal():
    # Given a NewPrepayment object
    period_start = 2
    new = general.NewPrepayment(
        amount=600,
        nominal="abc",
        period_start=2,
        periods=3,
        description="some description",
        description_recurring="monthly abc",
    )
    # When creating prepayment
    periods = GeneralLedger(ledger=None, chart_of_accounts=None).periods
    jnls = general.create_prepayment_journal(new, periods)
    # Then default jnls created as
    assert jnls == [
        GLJournal(
            jnl_type="ppmt",
            transaction_date=datetime.datetime(2021, period_start, 1, 0, 0),
            lines=[
                GLJournalLine(
                    nominal="prepayments",
                    amount=new.amount,
                    description=new.description,
                    transaction_date=datetime.datetime(2021, period_start, 1, 0, 0),
                ),
                GLJournalLine(
                    nominal=new.nominal,
                    amount=-new.amount,
                    description=new.description,
                    transaction_date=datetime.datetime(2021, period_start, 1, 0, 0),
                ),
            ],
        ),
        GLJournal(
            jnl_type="ppmt",
            transaction_date=datetime.datetime(2021, period_start + 1, 1, 0, 0),
            lines=[
                GLJournalLine(
                    nominal="prepayments",
                    amount=-200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 1, 1, 0, 0),
                ),
                GLJournalLine(
                    nominal=new.nominal,
                    amount=200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 1, 1, 0, 0),
                ),
            ],
        ),
        GLJournal(
            jnl_type="ppmt",
            transaction_date=datetime.datetime(2021, period_start + 2, 1, 0, 0),
            lines=[
                GLJournalLine(
                    nominal="prepayments",
                    amount=-200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 2, 1, 0, 0),
                ),
                GLJournalLine(
                    nominal=new.nominal,
                    amount=200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 2, 1, 0, 0),
                ),
            ],
        ),
        GLJournal(
            jnl_type="ppmt",
            transaction_date=datetime.datetime(2021, period_start + 3, 1, 0, 0),
            lines=[
                GLJournalLine(
                    nominal="prepayments",
                    amount=-200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 3, 1, 0, 0),
                ),
                GLJournalLine(
                    nominal=new.nominal,
                    amount=200,
                    description=new.description_recurring,
                    transaction_date=datetime.datetime(2021, period_start + 3, 1, 0, 0),
                ),
            ],
        ),
    ]


def test_create_prepayment_journal_balancing_amount():
    # Given a NewPrepayment where amount / periods is not an integer
    new = general.NewPrepayment(
        amount=700,
        nominal="abc",
        period_start=2,
        periods=3,
        description="some description",
        description_recurring="monthly abc",
    )
    # When creating prepayment journals
    periods = GeneralLedger(ledger=None, chart_of_accounts=None).periods
    jnls = general.create_prepayment_journal(new, periods)
    # Then total of journal lines by nominal equals zero
    prepayment_balance, nominal_balance = 0, 0
    for jnl in jnls:
        for line in jnl.lines:
            if line.nominal == "prepayments":
                prepayment_balance += line.amount
            if line.nominal == new.nominal:
                nominal_balance += line.amount
    assert prepayment_balance == 0
    assert nominal_balance == 0
