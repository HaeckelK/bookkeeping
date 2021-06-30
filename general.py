from dataclasses import dataclass, asdict
from typing import List, Dict
from abc import ABC, abstractmethod

import pandas as pd

from ledger import PandasLedger
from utils import convert_date_string_to_period


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
        return
