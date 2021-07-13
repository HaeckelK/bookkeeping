from typing import List, Dict
from dataclasses import dataclass
from abc import ABC, abstractmethod


@dataclass
class DispersalConfig:
    source: str
    target: str
    is_aggregated: bool
    is_reversed: bool


@dataclass
class DispersalLog:
    source: str
    target: str
    is_aggregated: bool
    is_reversed: bool
    source_ids: List[int]
    target_ids: List[int]


class LedgerTransaction:
    pass

# TODO put into ledgers
class TransactionsLedger(ABC):
    @abstractmethod
    def list_transactions(self) -> List[LedgerTransaction]:
        """"""


class InMemoryTransactionLedger(TransactionsLedger):
    def __init__(self) -> None:
        self.transactions = []
        return

    def list_transactions(self) -> List[LedgerTransaction]:
        return self.transactions


class DispersalsLogger:
    def __init__(self) -> None:
        self._ledgers: Dict[str, TransactionsLedger] = {}
        self._dispersed_ids: Dict[str, List[int]] = {}
        return

    def register_ledger(self, name: str, ledger_transactions: TransactionsLedger) -> None:
        self._ledgers[name] = ledger_transactions
        self._dispersed_ids[name] = []
        return

    @property
    def ledger_names(self) -> List[str]:
        return sorted(list(self._ledgers.keys()))

    # TODO this method doesn't belong in this class
    # TODO needs to specify target
    def undispersed_transactions(self, name: str) -> List[LedgerTransaction]:
        # TODO remove items already dispersed
        dispersed_ids = self._dispersed_ids[name]
        return [x for x in self._ledgers[name].list_transactions() if x.transaction_id not in dispersed_ids]

    # TODO needs to specify target
    def log_dispersal(self, name: str, transactions: List[LedgerTransaction]) -> None:
        # TODO can't add duplicates
        self._dispersed_ids[name].extend([x.transaction_id for x in transactions])
        return
