from dataclasses import dataclass, asdict
from typing import List

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


class GeneralLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = ["transaction_id", "jnl_id", "nominal", "jnl_type", "amount", "description", "transaction_date"]
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
        self.append(df)
        return
