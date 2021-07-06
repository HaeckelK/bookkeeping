from dataclasses import dataclass, asdict
from typing import List, Dict
from abc import ABC, abstractmethod
from copy import copy

import pandas as pd

from ledger import PandasLedger
from utils import convert_date_string_to_period


class JournalBalanceError(Exception):
    pass


@dataclass
class Period:
    period: int
    date_start: str
    date_end: str


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

    @property
    def total(self) -> int:
        return sum(x.amount for x in self.lines)


def create_opposite_journal(journal: GLJournal) -> GLJournal:
    new_lines = []
    for line in journal.lines:
        new_line = copy(line)
        new_line.amount = new_line.amount * -1
        new_lines.append(new_line)

    new_journal = GLJournal(jnl_type=journal.jnl_type, lines=new_lines)
    return new_journal


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
        self.columns = [
            "transaction_id",
            "jnl_id",
            "jnl_type",
            "transaction_date",
            "period",
            "nominal",
            "amount",
            "description",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def get_next_journal_id(self) -> int:
        try:
            next_id = int(self.df["jnl_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def add_journal(self, journal: GLJournal) -> List[int]:
        if journal.total != 0:
            print(journal)
            raise JournalBalanceError(f"Journal does not balance: {journal.total}")
        df = pd.DataFrame([asdict(x) for x in journal.lines])
        df["jnl_type"] = journal.jnl_type
        df["jnl_id"] = self.get_next_journal_id()
        # TODO Period should be supplied with journal
        df["period"] = df["transaction_date"].apply(convert_date_string_to_period)
        transaction_ids = self.append(df)
        return transaction_ids

    def list_transactions(self) -> List[GeneralLedgerTransaction]:
        return [GeneralLedgerTransaction(**x) for x in self.df.to_dict("records")]

    @property
    def balance(self) -> int:
        return self.df["amount"].sum()

    @property
    def balances(self) -> Dict[str, int]:
        data = self.df[["nominal", "amount"]].groupby(["nominal"]).sum().to_dict()["amount"]
        return data


@dataclass
class NewNominal:
    name: str
    statement: str
    heading: str
    expected_sign: str
    control_account: bool
    bank_account: bool


@dataclass
class Nominal:
    name: str
    statement: str
    heading: str
    expected_sign: str
    control_account: bool
    bank_account: bool


# TODO don't allow same nominal to be added more than once
class ChartOfAccounts(ABC):
    @abstractmethod
    def add_nominal(self, nominal: NewNominal) -> None:
        """Add a new nominal account to Chart Of Accounts."""

    @property
    @abstractmethod
    def nominals(self) -> List[Nominal]:
        """"""


class InMemoryChartOfAccounts(ChartOfAccounts):
    def __init__(self) -> None:
        self._nominals: Nominal = []
        return

    def add_nominal(self, nominal: NewNominal) -> None:
        self._nominals.append(Nominal(**asdict(nominal)))
        return

    @property
    def nominals(self) -> List[Nominal]:
        return [x for x in self._nominals]


class GeneralLedger:
    def __init__(self, ledger: GeneralLedgerTransactions, chart_of_accounts: ChartOfAccounts):
        self.ledger = ledger
        self.chart_of_accounts = chart_of_accounts
        self.periods = {1: Period(period=1, date_start="01/01/2021", date_end="31/01/21"),
                        2: Period(period=2, date_start="01/02/2021", date_end="28/02/21"),
                        3: Period(period=3, date_start="01/03/2021", date_end="31/03/21"),
                        4: Period(period=4, date_start="01/04/2021", date_end="30/04/21"),
                        5: Period(period=5, date_start="01/05/2021", date_end="31/05/21"),
                        6: Period(period=6, date_start="01/06/2021", date_end="30/06/21"),
                        7: Period(period=7, date_start="01/07/2021", date_end="31/07/21"),
                        8: Period(period=8, date_start="01/08/2021", date_end="31/08/21"),
                        9: Period(period=9, date_start="01/09/2021", date_end="30/09/21"),
                        10: Period(period=10, date_start="01/10/2021", date_end="31/10/21"),
                        11: Period(period=11, date_start="01/11/2021", date_end="30/11/21"),
                        12: Period(period=12, date_start="01/12/2021", date_end="31/12/21"),}
        return
