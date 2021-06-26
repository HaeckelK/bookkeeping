from abc import ABC, abstractmethod
from typing import List

import numpy as np


class Ledger(ABC):
    @abstractmethod
    def get_next_batch_id(self) -> int:
        """Return next available batch id."""

    @abstractmethod
    def get_next_transaction_id(self) -> int:
        """Return next available transaction id."""


class PandasLedger(Ledger):
    def get_next_batch_id(self) -> int:
        try:
            next_id = int(self.df["batch_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def append(self, df) -> List[int]:
        next_id = self.get_next_transaction_id()
        ids = np.arange(start=next_id, stop=next_id + df.shape[0])
        df["transaction_id"] = ids
        self.df = self.df.append(df[self.columns], ignore_index=True, sort=False)
        return list(ids)

    def get_next_transaction_id(self) -> int:
        try:
            next_id = int(self.df["transaction_id"].max()) + 1
        except ValueError:
            return 0
        return next_id
