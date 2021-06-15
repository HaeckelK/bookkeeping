from abc import ABC, abstractmethod

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

    def append(self, df):
        next_id = self.get_next_transaction_id()
        df["transaction_id"] = np.arange(start=next_id, stop=next_id + df.shape[0])
        self.df = self.df.append(df[self.columns], ignore_index=True, sort=False)
        return

    def get_next_transaction_id(self) -> int:
        try:
            next_id = int(self.df["transaction_id"].max()) + 1
        except ValueError:
            return 0
        return next_id
