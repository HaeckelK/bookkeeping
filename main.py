from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import List

import pandas as pd
import numpy as np


class SourceDataLoader:
    def load(self, filename: str, sheetname: str):
        df = pd.read_excel(filename, sheet_name=sheetname, index_col=None)
        df["Amount"] = df["Amount"] * 100
        df = df.astype({'Amount': 'int32'})
        df.insert(0, 'raw_id', range(0, 0 + len(df)))
        # df = df.set_index('raw_id')
        return df


class SourceDataParser:
    def register_source_data(self, df) -> None:
        self.df = df
        return

    def get_bank_transactions(self):
        # TODO return dictionary where key is bank_code
        bank = self.df[['Date', 'Transaction type', 'Description', 'Amount', 'Transfer Type', 'raw_id']]
        return bank

    def get_settled_invoices(self):
        df = self.df[['raw_id', 'Date', 'Amount', 'Creditor', 'PL', 'Notes']]
        df = df.loc[(df['Creditor'].notnull()) & (df['PL'].notnull())]
        return df

    def get_settled_sales_invoices(self):
        df = self.df[['raw_id', 'Date', 'Amount', 'Debtor', 'PL', 'Notes']]
        df = df.loc[(df['Debtor'].notnull()) & (df['PL'].notnull())]
        return df

    def get_unmatched_payments(self):
        df = self.df.copy()
        df = df.loc[(df['Creditor'].notnull()) & (df['PL'].isnull()) & (df['BS'].isnull())]
        df = df[['raw_id', 'Date', 'Amount', 'Creditor', 'Notes', 'Bank']]
        return df

    def get_unmatched_receipts(self):
        df = self.df.copy()
        df = df.loc[(df['Debtor'].notnull()) & (df['PL'].isnull()) & (df['BS'].isnull())]
        df = df[['raw_id', 'Date', 'Amount', 'Debtor', 'Notes', 'Bank']]
        return df


class Ledger(ABC):
    @abstractmethod
    def get_next_batch_id(self) -> int:
        """Return next available batch id."""


class PandasLedger(Ledger):
    def get_next_batch_id(self) -> int:
        try:
            next_id = int(self.df["batch_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def append(self, df):
        self.df = self.df.append(df, ignore_index=True, sort=False)
        return


class BankLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = ['raw_id', 'batch_id', 'bank_code', 'Date', 'Transaction type', 'Description',
                        'Amount', 'Transfer Type', "gl_jnl"]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_transactions(self, transactions, bank_code: str):
        df = transactions.copy()
        df['batch_id'] = self.get_next_batch_id()
        df["bank_code"] = bank_code
        df["gl_jnl"] = False
        self.append(df)
        return


class PurchaseLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = ['raw_id', 'batch_id', 'entry_type', 'Creditor', 'Date', 'Amount', 'Notes',
                                        'gl_jnl', 'settled']
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_settled_transcations(self, settled_invoices, bank_code: str):
        batch_id = self.get_next_batch_id()
        df = settled_invoices.copy()
        df['batch_id'] = batch_id
        df['entry_type'] = 'bank_payment'
        df['Notes'] = f'bank payment {bank_code}'
        df["gl_jnl"] = False
        df["settled"] = True
        self.append(df)

        df = settled_invoices.copy()
        df['batch_id'] = batch_id
        df['Amount'] = -df['Amount']
        df['entry_type'] = 'purchase_invoice'
        df["gl_jnl"] = False
        df["settled"] = True
        self.append(df)
        return

    def add_payments(self, payments):
        batch_id = self.get_next_batch_id()
        df = payments.copy()
        df['batch_id'] = batch_id
        df['entry_type'] = 'bank_payment'
        df['Notes'] = 'bank payment ' + df['Bank']
        df["gl_jnl"] = False
        df["settled"] = False
        df = df.drop(labels="Bank", axis=1)
        self.append(df)
        return


# TODO parent calss for SalesLedger, PurchaseLedger
class SalesLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = ['raw_id', 'batch_id', 'entry_type', 'Debtor', 'Date', 'Amount', 'Notes',
                                        'gl_jnl', 'settled']
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_settled_transcations(self, settled_invoices, bank_code: str):
        batch_id = self.get_next_batch_id()
        df = settled_invoices.copy()
        df['batch_id'] = batch_id
        df['entry_type'] = 'bank_receipt'
        df['Amount'] = -df['Amount']
        df['Notes'] = f'bank receipt {bank_code}'
        df["gl_jnl"] = False
        df["settled"] = True
        self.append(df)

        df = settled_invoices.copy()
        df['batch_id'] = batch_id
        df['entry_type'] = 'sale_invoice'
        df["gl_jnl"] = False
        df["settled"] = True
        self.append(df)
        return

    def add_receipts(self, payments):
        batch_id = self.get_next_batch_id()
        df = payments.copy()
        df['batch_id'] = batch_id
        df['entry_type'] = 'bank_receipt'
        df['Notes'] = 'bank receipt ' + df['Bank']
        df["gl_jnl"] = False
        df["settled"] = False
        df = df.drop(labels="Bank", axis=1)
        self.append(df)
        return


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


class GeneralLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = ['transaction_id', 'jnl_id', 'nominal', 'jnl_type', 'amount', 'description', 'transaction_date']
        self.df = pd.DataFrame(columns=self.columns)
        return

    def get_next_transaction_id(self) -> int:
        try:
            next_id = int(self.df["transaction_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def get_next_journal_id(self) -> int:
        try:
            next_id = int(self.df["jnl_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def add_journal(self, journal: GLJournal) -> None:
        df = pd.DataFrame([asdict(x) for x in journal.lines])
        df["jnl_type"] = journal.jnl_type
        next_id = self.get_next_transaction_id()
        df['transaction_id'] = np.arange(start=next_id, stop=next_id+df.shape[0])
        df['jnl_id'] = self.get_next_journal_id()
        self.append(df)
        return


def main():
    data_loader = SourceDataLoader()
    parser = SourceDataParser()
    bank_ledger = BankLedger()
    purchase_ledger = PurchaseLedger()
    sales_ledger = SalesLedger()
    general_ledger = GeneralLedger()

    print("Bookkeeping Demo")
    print("Load source excel")
    raw_data = data_loader.load("data/cashbook.xlsx", "bank")
    parser.register_source_data(raw_data)

    bank_transactions = parser.get_bank_transactions()
    settled_invoices = parser.get_settled_invoices()
    settled_sales_invoices = parser.get_settled_sales_invoices()
    unmatched_payments = parser.get_unmatched_payments()
    unmatched_receipts = parser.get_unmatched_receipts()

    # TODO do this in batches
    bank_ledger.add_transactions(bank_transactions, bank_code="nwa_ca")

    purchase_ledger.add_settled_transcations(settled_invoices, bank_code="nwa_ca")
    purchase_ledger.add_payments(unmatched_payments)

    sales_ledger.add_settled_transcations(settled_sales_invoices, bank_code="nwa_ca")
    sales_ledger.add_receipts(unmatched_receipts)

    journal_lines = [GLJournalLine(nominal="purchase_control_account",
                                  description="some description",
                                  amount=-999,
                                  transaction_date="XYZ"),
                     GLJournalLine(nominal="gym",
                                  description="some gym cost",
                                  amount=999,
                                  transaction_date="XYZ")]
    journal = GLJournal(jnl_type="si", lines=journal_lines)
    general_ledger.add_journal(journal)
    general_ledger.add_journal(journal)
    print(general_ledger.df)


    bank_ledger.df.to_csv("data/bank_ledger.csv", index=False)
    purchase_ledger.df.to_csv("data/purchase_ledger.csv", index=False)
    sales_ledger.df.to_csv("data/sales_ledger.csv", index=False)
    general_ledger.df.to_csv("data/general_ledger.csv", index=False)
    return


if __name__ == "__main__":
    main()
