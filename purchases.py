from dataclasses import dataclass, asdict
from typing import List

import pandas as pd

from ledger import PandasLedger


@dataclass
class NewPurchaseInvoiceLine:
    nominal: str
    description: str
    amount: int
    transaction_date: str
    raw_id: int


@dataclass
class NewPurchaseInvoice:
    creditor: str
    lines: List[NewPurchaseInvoiceLine]

    @property
    def total(self) -> int:
        return sum(x.amount for x in self.lines)


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
            "creditor",
            "date",
            "amount",
            "notes",
            "gl_jnl",
            "settled",
            "pl",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_invoices(self, invoices: List[NewPurchaseInvoice]) -> List[int]:
        batch_id = self.get_next_batch_id()
        transaction_ids = []
        for invoice in invoices:
            batch_id = self.get_next_batch_id()
            df = pd.DataFrame([asdict(x) for x in invoice.lines])
            # TODO rename in ledger definition
            df = df.rename(columns={"transaction_date": "date", "description": "notes", "nominal": "pl"})
            df["creditor"] = invoice.creditor
            df["batch_id"] = batch_id
            df["entry_type"] = "purchase_invoice"
            df["gl_jnl"] = False
            df["settled"] = False
            transaction_ids.extend(self.append(df))
        return transaction_ids

    def add_payments(self, payments: List[NewPurchaseLedgerPayment]) -> List[int]:
        batch_id = self.get_next_batch_id()
        df = pd.DataFrame([asdict(x) for x in payments])
        df["batch_id"] = batch_id
        df["entry_type"] = "bank_payment"
        df["notes"] = "bank payment " + df["bank_code"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["pl"] = None
        df = df.drop(labels="bank_code", axis=1)
        transaction_ids = self.append(df)
        return transaction_ids

    def allocate_transactions(self, transaction_ids: List[int]) -> None:
        return

    def get_unposted_invoices(self) -> List[PurchaseInvoice]:
        df = self.df.copy()
        df = df.loc[(df["gl_jnl"] == False) & (df["entry_type"] == "purchase_invoice")]
        invoices = []
        for invoice in df.to_dict("records"):
            credtior = invoice["creditor"]
            nominal = invoice["pl"]
            description = invoice["notes"]
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
