from dataclasses import dataclass, asdict
from typing import List
from abc import abstractmethod

import pandas as pd

from ledger import PandasLedger, Ledger


@dataclass
class RawBankTransaction:
    raw_id: int
    bank_code: str
    transfer_type: str
    transaction_type: str
    description: str
    amount: int
    date: str


@dataclass
class BankTransaction(RawBankTransaction):
    transaction_id: int
    batch_id: int
    gl_jnl: bool


class BankLedger(Ledger):
    @abstractmethod
    def add_transactions(self, transactions: List[RawBankTransaction]):
        """"""

    @abstractmethod
    def list_transactions(self) -> List[BankTransaction]:
        """"""


class InMemoryBankLedger(BankLedger, PandasLedger):
    def __init__(self) -> None:
        self.columns = [
            "transaction_id",
            "batch_id",
            "raw_id",
            "transfer_type",
            "date",
            "bank_code",
            "transaction_type",
            "amount",
            "description",
            "gl_jnl",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_transactions(self, transactions: List[RawBankTransaction]):
        df = pd.DataFrame([asdict(x) for x in transactions])
        df["batch_id"] = self.get_next_batch_id()
        df["gl_jnl"] = False
        self.append(df)
        return

    def list_transactions(self) -> List[BankTransaction]:
        return [BankTransaction(**x) for x in self.df.to_dict("records")]
