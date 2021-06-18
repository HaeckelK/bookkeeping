from dataclasses import dataclass, asdict
from typing import List, Dict

import pandas as pd

from ledger import PandasLedger


@dataclass
class GLJournalLine:
    nominal: str
    description: str
    amount: int
    transaction_date: str


@dataclass
class GLJournal:
    jnl_type: str
    lines: List[GLJournalLine]


@dataclass
class GeneralLedgerTransaction:
    transaction_id: int
    jnl_id: int
    nominal: str
    jnl_type: str
    amount: int
    description: str
    transaction_date: str
    period: int


class GeneralLedgerTransactions(PandasLedger):
    def __init__(self) -> None:
        self.columns = ["transaction_id", "jnl_id", "jnl_type", "transaction_date", "period", "nominal", "amount", "description"]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def get_next_journal_id(self) -> int:
        try:
            next_id = int(self.df["jnl_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def add_journal(self, journal: GLJournal) -> None:
        df = pd.DataFrame([asdict(x) for x in journal.lines])
        df["jnl_type"] = journal.jnl_type
        df["jnl_id"] = self.get_next_journal_id()
        # TODO Period should be supplied with journal
        df["period"] = df['transaction_date'].apply(convert_date_string_to_period)
        self.append(df)
        return

    def list_transactions(self) -> List[GeneralLedgerTransaction]:
        return [GeneralLedgerTransaction(**x) for x in self.df.to_dict("records")]

    @property
    def balance(self) -> int:
        return self.df["amount"].sum()

    @property
    def balances(self) -> Dict[str, int]:
        data = self.df[['nominal', 'amount']].groupby(['nominal']).sum().to_dict()["amount"]
        return data


def convert_date_string_to_period(timestamp) -> int:
    try:
        month = int(timestamp.month)
    except AttributeError:
        return -1
    else:
        return month


class GeneralLedger:
    def __init__(self, ledger: GeneralLedgerTransactions):
        self.ledger = ledger
        return
