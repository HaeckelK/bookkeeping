import pandas as pd

from ledger import PandasLedger


class BankLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = [
            "transaction_id",
            "raw_id",
            "batch_id",
            "bank_code",
            "Date",
            "Transaction type",
            "Description",
            "Amount",
            "Transfer Type",
            "gl_jnl",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_transactions(self, transactions, bank_code: str):
        df = transactions.copy()
        df["batch_id"] = self.get_next_batch_id()
        df["bank_code"] = bank_code
        df["gl_jnl"] = False
        self.append(df)
        return
