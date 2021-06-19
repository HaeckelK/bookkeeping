from dataclasses import dataclass, asdict
from typing import List

import pandas as pd

from ledger import PandasLedger


@dataclass
class PurchaseInvoiceLine:
    nominal: str
    description: str
    amount: int
    transaction_date: str


@dataclass
class PurchaseInvoice:
    creditor: str
    lines: List[PurchaseInvoiceLine]

    @property
    def total(self) -> int:
        return sum(x.amount for x in self.lines)


@dataclass
class NewPurchaseLedgerPayment:
    raw_id: int
    date: str
    amount: int
    creditor: str
    bank_code: str


class PurchaseLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = [
            "transaction_id",
            "raw_id",
            "batch_id",
            "entry_type",
            "Creditor",
            "date",
            "amount",
            "Notes",
            "gl_jnl",
            "settled",
            "PL",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_settled_transcations(self, settled_invoices):
        bank_codes = settled_invoices["bank_code"].unique()
        for bank_code in bank_codes:
            batch_id = self.get_next_batch_id()
            df = settled_invoices.copy()

            df["batch_id"] = batch_id
            df["entry_type"] = "bank_payment"
            df["Notes"] = f"bank payment {bank_code}"
            df["gl_jnl"] = False
            df["settled"] = True

            self.append(df)

            df = settled_invoices.copy()
            df["batch_id"] = batch_id
            df["amount"] = -df["amount"]
            df["entry_type"] = "purchase_invoice"
            df["gl_jnl"] = False
            df["settled"] = True
            self.append(df)
        return

    def add_payments(self, payments: List[NewPurchaseLedgerPayment]):
        batch_id = self.get_next_batch_id()
        df = pd.DataFrame([asdict(x) for x in payments])
        # TODO change columns to lower case in Ledger definition
        df = df.rename(columns={"creditor": "Creditor"})
        df["batch_id"] = batch_id
        df["entry_type"] = "bank_payment"
        df["Notes"] = "bank payment " + df["bank_code"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["PL"] = None
        df = df.drop(labels="bank_code", axis=1)
        self.append(df)
        return

    def get_unposted_invoices(self) -> List[PurchaseInvoice]:
        df = self.df.copy()
        df = df.loc[(df["gl_jnl"] == False) & (df["entry_type"] == "purchase_invoice")]
        invoices = []
        for invoice in df.to_dict("records"):
            credtior = invoice["Creditor"]
            nominal = invoice["PL"]
            description = invoice["Notes"]
            amount = -invoice["amount"]
            transaction_date = invoice["date"]

            purchase_invoice = PurchaseInvoice(
                creditor=credtior,
                lines=[
                    PurchaseInvoiceLine(
                        nominal=nominal, description=description, amount=amount, transaction_date=transaction_date
                    )
                ],
            )
            invoices.append(purchase_invoice)
        return invoices

    @property
    def balance(self) -> int:
        return self.df["amount"].sum()
