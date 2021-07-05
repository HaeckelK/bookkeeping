from general import GLJournal, GLJournalLine
import general


def test_create_opposite_journal():
    # Given a journal with multiple lines with non zero amount
    journal = GLJournal(jnl_type="gnl", lines=[GLJournalLine(nominal="abc",
                                                             description="description for abc",
                                                             amount=123,
                                                             transaction_date="01/01/1990"),
                                               GLJournalLine(nominal="def",
                                                             description="description for def",
                                                             amount=500,
                                                             transaction_date="01/01/1990"),
                                               GLJournalLine(nominal="ghi",
                                                             description="description for ghi",
                                                             amount=-623,
                                                             transaction_date="01/01/1990")])
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
