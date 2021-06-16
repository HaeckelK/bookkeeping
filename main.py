from dataclasses import dataclass, asdict
from typing import List
from abc import ABC, abstractmethod

import pandas as pd

from ledger import PandasLedger
from bank import BankLedger, InMemoryBankLedger, RawBankTransaction


class SourceDataLoader:
    def load(self, filename: str, sheetname: str):
        df = pd.read_excel(filename, sheet_name=sheetname, index_col=None)
        df["amount"] = df["amount"] * 100
        df = df.astype({"amount": "int32"})
        df.insert(0, "raw_id", range(0, 0 + len(df)))
        # df = df.set_index('raw_id')
        return df


class SourceDataParser:
    def register_source_data(self, df) -> None:
        self.df = df
        return

    def get_bank_transactions(self) -> List[RawBankTransaction]:
        df = self.df[["date", "transaction_type", "description", "amount", "transfer_type", "raw_id", "bank"]]
        lines = []
        for transaction in df.to_dict("records"):
            transaction["bank_code"] = transaction["bank"]
            del transaction["bank"]
            lines.append(RawBankTransaction(**transaction))
        return lines

    def get_settled_invoices(self):
        df = self.df[["raw_id", "date", "amount", "Creditor", "PL", "Notes"]]
        df = df.loc[(df["Creditor"].notnull()) & (df["PL"].notnull())]
        return df

    def get_settled_sales_invoices(self):
        df = self.df[["raw_id", "date", "amount", "Debtor", "PL", "Notes"]]
        df = df.loc[(df["Debtor"].notnull()) & (df["PL"].notnull())]
        return df

    def get_unmatched_payments(self):
        df = self.df.copy()
        df = df.loc[(df["Creditor"].notnull()) & (df["PL"].isnull()) & (df["BS"].isnull())]
        df = df[["raw_id", "date", "amount", "Creditor", "Notes", "bank"]]
        return df

    def get_unmatched_receipts(self):
        df = self.df.copy()
        df = df.loc[(df["Debtor"].notnull()) & (df["PL"].isnull()) & (df["BS"].isnull())]
        df = df[["raw_id", "date", "amount", "Debtor", "Notes", "bank"]]
        return df


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

    def add_settled_transcations(self, settled_invoices, bank_code: str):
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

    def add_payments(self, payments):
        batch_id = self.get_next_batch_id()
        df = payments.copy()
        df["batch_id"] = batch_id
        df["entry_type"] = "bank_payment"
        df["Notes"] = "bank payment " + df["bank"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["PL"] = None
        df = df.drop(labels="bank", axis=1)
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


# TODO parent calss for SalesLedger, PurchaseLedger
class SalesLedger(PandasLedger):
    def __init__(self) -> None:
        self.columns = [
            "transaction_id",
            "raw_id",
            "batch_id",
            "entry_type",
            "Debtor",
            "date",
            "amount",
            "Notes",
            "gl_jnl",
            "settled",
            "PL",
        ]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def add_settled_transcations(self, settled_invoices, bank_code: str):
        batch_id = self.get_next_batch_id()
        df = settled_invoices.copy()
        df["batch_id"] = batch_id
        df["entry_type"] = "bank_receipt"
        df["amount"] = -df["amount"]
        df["Notes"] = f"bank receipt {bank_code}"
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
        df["Notes"] = "bank receipt " + df["bank"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["PL"] = None
        df = df.drop(labels="bank", axis=1)
        self.append(df)
        return

    def get_unposted_invoices(self) -> List[SalesInvoice]:
        df = self.df.copy()
        df = df.loc[(df["gl_jnl"] == False) & (df["entry_type"] == "sale_invoice")]
        invoices = []
        for invoice in df.to_dict("records"):
            credtior = invoice["Debtor"]
            nominal = invoice["PL"]
            description = invoice["Notes"]
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
        self.columns = ["transaction_id", "jnl_id", "nominal", "jnl_type", "amount", "description", "transaction_date"]
        self.df = pd.DataFrame(columns=self.columns)
        return

    def get_next_journal_id(self) -> int:
        try:
            next_id = int(self.df["jnl_id"].max()) + 1
        except ValueError:
            return 0
        return next_id

    def add_journal(self, journal: GLJournal) -> None:
        df = pd.DataFrame([asdict(x) for x in journal.lines])
        df["jnl_type"] = journal.jnl_type
        df["jnl_id"] = self.get_next_journal_id()
        self.append(df)
        return


class InterLedgerJournalCreator:
    def create_pl_to_gl_journals(self, invoices: List[PurchaseInvoice]) -> List[GLJournal]:
        total = sum(x.total for x in invoices)

        gl_lines = [
            GLJournalLine(
                nominal="puchase_ledger_control_account",
                description="some auto generated description",
                amount=-total,
                transaction_date="TODAY",
            )
        ]
        for invoice in invoices:
            for line in invoice.lines:
                gl_line = GLJournalLine(
                    nominal=line.nominal,
                    description=line.description,
                    amount=line.amount,
                    transaction_date=line.transaction_date,
                )
                gl_lines.append(gl_line)

        journal = GLJournal(jnl_type="pi", lines=gl_lines)

        return [journal]

    # TODO DRY see create_pl_to_gl_journals
    def create_sl_to_gl_journals(self, invoices: List[PurchaseInvoice]) -> List[GLJournal]:
        total = sum(x.total for x in invoices)

        gl_lines = [
            GLJournalLine(
                nominal="sales_ledger_control_account",
                description="some auto generated description",
                amount=-total,
                transaction_date="TODAY",
            )
        ]
        for invoice in invoices:
            for line in invoice.lines:
                gl_line = GLJournalLine(
                    nominal=line.nominal,
                    description=line.description,
                    amount=line.amount,
                    transaction_date=line.transaction_date,
                )
                gl_lines.append(gl_line)

        journal = GLJournal(jnl_type="si", lines=gl_lines)

        return [journal]


class ReportWriter(ABC):
    @abstractmethod
    def write_bank_ledger(self, ledger: BankLedger):
        """"""


class CSVReportWriter(ReportWriter):
    def write_bank_ledger(self, ledger: BankLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_csv("data/bank_ledger.csv", index=False)
        return


class HTMLReportWriter(ReportWriter):
    def write_bank_ledger(self, ledger: BankLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_html("data/html/bank_ledger.html", index=False)
        return


def main():
    data_loader = SourceDataLoader()
    parser = SourceDataParser()
    bank_ledger = InMemoryBankLedger()
    purchase_ledger = PurchaseLedger()
    sales_ledger = SalesLedger()
    general_ledger = GeneralLedger()
    inter_ledger_jnl_creator = InterLedgerJournalCreator()
    report_writer = HTMLReportWriter()

    print("Bookkeeping Demo")
    print("Load source excel")
    raw_data = data_loader.load("data/cashbook.xlsx", "bank")
    parser.register_source_data(raw_data)

    bank_transactions = parser.get_bank_transactions()
    settled_invoices = parser.get_settled_invoices()
    settled_sales_invoices = parser.get_settled_sales_invoices()
    unmatched_payments = parser.get_unmatched_payments()
    unmatched_receipts = parser.get_unmatched_receipts()

    bank_ledger.add_transactions(bank_transactions)

    purchase_ledger.add_settled_transcations(settled_invoices, bank_code="nwa_ca")
    purchase_ledger.add_payments(unmatched_payments)

    sales_ledger.add_settled_transcations(settled_sales_invoices, bank_code="nwa_ca")
    sales_ledger.add_receipts(unmatched_receipts)

    journals = inter_ledger_jnl_creator.create_pl_to_gl_journals(purchase_ledger.get_unposted_invoices())
    for journal in journals:
        general_ledger.add_journal(journal)
        # TODO update purchase_ledger that these have been added to gl

    journals = inter_ledger_jnl_creator.create_sl_to_gl_journals(sales_ledger.get_unposted_invoices())
    for journal in journals:
        general_ledger.add_journal(journal)
        # TODO update sales_ledger that these have been added to gl

    print(general_ledger.df)

    report_writer.write_bank_ledger(bank_ledger)
    purchase_ledger.df.to_csv("data/purchase_ledger.csv", index=False)
    sales_ledger.df.to_csv("data/sales_ledger.csv", index=False)
    general_ledger.df.to_csv("data/general_ledger.csv", index=False)
    return


if __name__ == "__main__":
    main()
