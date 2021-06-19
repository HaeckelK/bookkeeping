from dataclasses import dataclass
from typing import List

import pandas as pd

from ledger import PandasLedger


@dataclass
class SalesInvoiceLine:
    nominal: str
    description: str
    amount: int
    transaction_date: str


@dataclass
class SalesInvoice:
    creditor: str
    lines: List[SalesInvoiceLine]

    @property
    def total(self) -> int:
        return sum(x.amount for x in self.lines)


# TODO parent calss for SalesLedger, PurchaseLedger
class SalesLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = [
            "transaction_id",
            "raw_id",
            "batch_id",
            "entry_type",
            "debtor",
            "date",
            "amount",
            "notes",
            "gl_jnl",
            "settled",
            "pl",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_settled_transcations(self, settled_invoices):
        bank_codes = settled_invoices["bank_code"].unique()
        for bank_code in bank_codes:
            batch_id = self.get_next_batch_id()
            df = settled_invoices.copy()
            df["batch_id"] = batch_id
            df["entry_type"] = "bank_receipt"
            df["amount"] = -df["amount"]
            df["notes"] = f"bank receipt {bank_code}"
            df["gl_jnl"] = False
            df["settled"] = True
            self.append(df)

            df = settled_invoices.copy()
            df["batch_id"] = batch_id
            df["entry_type"] = "sale_invoice"
            df["gl_jnl"] = False
            df["settled"] = True
            self.append(df)
        return

    def add_receipts(self, payments):
        batch_id = self.get_next_batch_id()
        df = payments.copy()
        df["batch_id"] = batch_id
        df["entry_type"] = "bank_receipt"
        df["notes"] = "bank receipt " + df["bank_code"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["pl"] = None
        df = df.drop(labels="bank_code", axis=1)
        self.append(df)
        return

    def get_unposted_invoices(self) -> List[SalesInvoice]:
        df = self.df.copy()
        df = df.loc[(df["gl_jnl"] == False) & (df["entry_type"] == "sale_invoice")]
        invoices = []
        for invoice in df.to_dict("records"):
            credtior = invoice["debtor"]
            nominal = invoice["pl"]
            description = invoice["notes"]
            amount = invoice["amount"]
            transaction_date = invoice["date"]

            purchase_invoice = SalesInvoice(
                creditor=credtior,
                lines=[
                    SalesInvoiceLine(
                        nominal=nominal, description=description, amount=amount, transaction_date=transaction_date
                    )
                ],
            )
            invoices.append(purchase_invoice)
        return invoices

    @property
    def balance(self) -> int:
        return self.df["amount"].sum()
