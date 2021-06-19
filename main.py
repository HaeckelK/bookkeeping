from dataclasses import dataclass, asdict
from typing import List
import datetime

import pandas as pd

from ledger import PandasLedger
from general import GLJournal, GLJournalLine, GeneralLedgerTransactions, GeneralLedger, InMemoryChartOfAccounts, NewNominal
from bank import BankTransaction, InMemoryBankLedger, RawBankTransaction
from reporting import HTMLReportWriter


class ExcelSourceDataLoader:
    def __init__(self, filename: str, bank_sheet: str, coa_sheet: str) -> None:
        self.filename = filename
        self.bank_sheet = bank_sheet
        self.coa_sheet = coa_sheet

        self.bank = None
        self.coa = None
        return

    def load(self):
        print("Loading Bank Sheet")
        self.load_bank()
        print("Loading COA Sheet")
        self.load_coa()
        return

    def load_bank(self):
        # TODO set all column names to lower
        df = pd.read_excel(self.filename, sheet_name=self.bank_sheet, index_col=None)
        df["amount"] = df["amount"] * 100
        df = df.astype({"amount": "int32"})
        df.insert(0, "raw_id", range(0, 0 + len(df)))
        self.bank = df
        return

    def load_coa(self):
        df = pd.read_excel(self.filename, sheet_name=self.coa_sheet, index_col=None)
        df["control_account"] = df["control_account"].map({'y': True, 'n': False})
        df["bank_account"] = df["bank_account"].map({'y': True, 'n': False})
        self.coa = df
        return


class SourceDataParser:
    def register_source_data(self, bank, coa) -> None:
        self.bank = bank
        self.coa = coa
        self.extend_coa()
        return

    def extend_coa(self) -> None:
        """self.coa may not be complete. Add any nominals seen in other sheets."""
        additional_nominals = []
        existing_nominals = self.coa["nominal"].unique()
        for field in ('BS', 'PL'):
            for bank_nominal in self.bank[field].unique():
                if isinstance(bank_nominal, str) is False:
                    continue
                if bank_nominal in existing_nominals:
                    continue
                additional_nominals.append({"nominal": bank_nominal, "statement": field.lower(),
                                            "expected_sign": "dr", "control_account": False,
                                            "bank_account": False, "heading": "NOMINAL DETAILS MISSING"})
        if additional_nominals:
            self.coa = self.coa.append(additional_nominals, ignore_index=True)
        return

    def get_bank_transactions(self) -> List[RawBankTransaction]:
        # TODO strip this out as a util manipulation
        matched = self.bank[["Creditor", "Debtor", "BS"]].copy()
        matched_dict = {}
        for index, line in matched.to_dict("index").items():
            for matched_type, matched_account in line.items():
                if isinstance(matched_account, str):
                    matched_dict[index] = [matched_account, matched_type.lower()]

        matched = pd.DataFrame.from_dict(matched_dict, orient='index', columns=["matched_account", "matched_type"])

        df = self.bank[["date", "transaction_type", "description", "amount", "transfer_type", "raw_id", "bank_code"]]
        df = df.join(matched)
        lines = []
        for transaction in df.to_dict("records"):
            lines.append(RawBankTransaction(**transaction))
        return lines

    def get_settled_invoices(self):
        df = self.bank[["raw_id", "date", "amount", "Creditor", "PL", "Notes", "bank_code"]]
        df = df.loc[(df["Creditor"].notnull()) & (df["PL"].notnull())]
        return df

    def get_settled_sales_invoices(self):
        df = self.bank[["raw_id", "date", "amount", "Debtor", "PL", "Notes", "bank_code"]]
        df = df.loc[(df["Debtor"].notnull()) & (df["PL"].notnull())]
        return df

    def get_unmatched_payments(self):
        df = self.bank.copy()
        df = df.loc[(df["Creditor"].notnull()) & (df["PL"].isnull()) & (df["BS"].isnull())]
        df = df[["raw_id", "date", "amount", "Creditor", "Notes", "bank_code"]]
        return df

    def get_unmatched_receipts(self):
        df = self.bank.copy()
        df = df.loc[(df["Debtor"].notnull()) & (df["PL"].isnull()) & (df["BS"].isnull())]
        df = df[["raw_id", "date", "amount", "Debtor", "Notes", "bank_code"]]
        return df

    @property
    def chart_of_accounts_config(self) -> List[NewNominal]:
        # TODO get accounts not listed in COA sheet, look at bank sheet too
        nominals = []
        for nominal in self.coa.to_dict("record"):
            nominal["name"] = nominal["nominal"]
            del nominal["nominal"]
            nominals.append(NewNominal(**nominal))
        return nominals


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

    def add_payments(self, payments):
        batch_id = self.get_next_batch_id()
        df = payments.copy()
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

    def add_settled_transcations(self, settled_invoices):
        bank_codes = settled_invoices["bank_code"].unique()
        for bank_code in bank_codes:
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
        df["Notes"] = "bank receipt " + df["bank_code"]
        df["gl_jnl"] = False
        df["settled"] = False
        df["PL"] = None
        df = df.drop(labels="bank_code", axis=1)
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

    @property
    def balance(self) -> int:
        return self.df["amount"].sum()


class InterLedgerJournalCreator:
    def create_pl_to_gl_journals(self, invoices: List[PurchaseInvoice]) -> List[GLJournal]:
        total = sum(x.total for x in invoices)
        transaction_date = max(line.transaction_date for invoice in invoices for line in invoice.lines)
        gl_lines = [
            GLJournalLine(
                nominal="purchase_ledger_control_account",
                description="some auto generated description",
                amount=-total,
                transaction_date=transaction_date,
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
        transaction_date = max(line.transaction_date for invoice in invoices for line in invoice.lines)
        gl_lines = [
            GLJournalLine(
                nominal="sales_ledger_control_account",
                description="some auto generated description",
                amount=-total,
                transaction_date=transaction_date,
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

    def create_bank_to_gl_journals(self, transactions: BankTransaction) -> List[GLJournal]:
        df = pd.DataFrame([asdict(x) for x in transactions])
        df = df[['bank_code', 'matched_type', 'amount']].groupby(['bank_code', 'matched_type']).sum().reset_index()

        lookup = {"creditor": "purchase_ledger_control_account",
                  "debtor": "sales_ledger_control_account",
                  "bs": "bank_contra"}
        journals = []
        for line in df.to_dict("record"):
            bank_code = line["bank_code"]
            amount = -line["amount"]
            gl_account = lookup[line["matched_type"]]

            gl_lines = [
                GLJournalLine(
                    nominal=bank_code,
                    description="some auto generated description",
                    amount=amount,
                    transaction_date=datetime.datetime.now(),
                ),
                GLJournalLine(
                    nominal=gl_account,
                    description="some auto generated description",
                    amount=-amount,
                    transaction_date=datetime.datetime.now(),
                )
            ]
            journal = GLJournal(jnl_type="bank", lines=gl_lines)
            journals.append(journal)
        return journals


def main():
    data_loader = ExcelSourceDataLoader(filename="data/cashbook.xlsx",
                                        bank_sheet="bank",
                                        coa_sheet="coa")
    parser = SourceDataParser()
    bank_ledger = InMemoryBankLedger()
    purchase_ledger = PurchaseLedger()
    sales_ledger = SalesLedger()
    general_ledger = GeneralLedgerTransactions()
    general = GeneralLedger(ledger=general_ledger,
                            chart_of_accounts=InMemoryChartOfAccounts())
    inter_ledger_jnl_creator = InterLedgerJournalCreator()
    report_writer = HTMLReportWriter(path="data/html")

    print("Bookkeeping Demo")
    print("Load source excel")
    data_loader.load()
    parser.register_source_data(bank=data_loader.bank,
                                coa=data_loader.coa)

    # Setup financials config
    nominals = parser.chart_of_accounts_config
    for nominal in nominals:
        print(f"Adding nominal to COA: {nominal.name}")
        general.chart_of_accounts.add_nominal(nominal)


    bank_transactions = parser.get_bank_transactions()
    settled_invoices = parser.get_settled_invoices()
    settled_sales_invoices = parser.get_settled_sales_invoices()
    unmatched_payments = parser.get_unmatched_payments()
    unmatched_receipts = parser.get_unmatched_receipts()

    bank_ledger.add_transactions(bank_transactions)

    purchase_ledger.add_settled_transcations(settled_invoices)
    purchase_ledger.add_payments(unmatched_payments)

    sales_ledger.add_settled_transcations(settled_sales_invoices)
    sales_ledger.add_receipts(unmatched_receipts)

    journals = inter_ledger_jnl_creator.create_pl_to_gl_journals(purchase_ledger.get_unposted_invoices())
    for journal in journals:
        general.ledger.add_journal(journal)
        # TODO update purchase_ledger that these have been added to gl

    journals = inter_ledger_jnl_creator.create_sl_to_gl_journals(sales_ledger.get_unposted_invoices())
    for journal in journals:
        general.ledger.add_journal(journal)
        # TODO update sales_ledger that these have been added to gl

    journals = inter_ledger_jnl_creator.create_bank_to_gl_journals(bank_ledger.list_transactions())
    for journal in journals:
        general.ledger.add_journal(journal)
        # TODO update bank_ledger that these have been added to gl

    # Reporting
    print("\nPublishing Report")
    print("Bank Ledger")
    report_writer.write_bank_ledger(bank_ledger)
    print("General Ledger")
    report_writer.write_general_ledger(general.ledger, general.chart_of_accounts)
    print("Purchase Ledger")
    report_writer.write_purchase_ledger(purchase_ledger)
    print("Sales Ledger")
    report_writer.write_sales_ledger(sales_ledger)

    # Validation
    print("Running validation checks")
    print("General Ledger sums to 0. Value:", general.ledger.balance, general.ledger.balance == 0)
    # TODO each bank account sums to account on GL

    print("\n")
    print("Purchase Ledger Control Account agrees to Purchase Ledger")
    plca_value = general.ledger.balances["purchase_ledger_control_account"]
    print("PLCA value:", plca_value)
    purchase_ledger_balance = purchase_ledger.balance
    print("Purchase Ledger value:", purchase_ledger_balance)
    print(plca_value == purchase_ledger_balance)
    print("\n")
    slca_value = general.ledger.balances["sales_ledger_control_account"]
    print("SLCA value:", slca_value)
    sales_ledger_balance = sales_ledger.balance
    print("Sales Ledger value:", sales_ledger_balance)
    print(slca_value == sales_ledger_balance)

    # TODO validate num raw transactions vs num bank ledger transactions

    return


if __name__ == "__main__":
    main()
