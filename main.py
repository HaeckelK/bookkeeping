from dataclasses import dataclass, asdict
from typing import List, Tuple
import datetime

import pandas as pd

from ledger import PandasLedger
from general import GLJournal, GLJournalLine, GeneralLedgerTransactions, GeneralLedger, InMemoryChartOfAccounts, NewNominal
from bank import BankTransaction, InMemoryBankLedgerTransactions, RawBankTransaction, BankLedger
from purchases import NewPurchaseInvoice, PurchaseInvoice, PurchaseLedger, NewPurchaseLedgerPayment
from sales import SalesLedger, NewSalesLedgerReceipt
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
        for field in ('bs', 'pl'):
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
        matched = self.bank[["creditor", "debtor", "bs"]].copy()
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

    def get_settled_purchase_invoices(self):
        df = self.bank[["raw_id", "date", "amount", "creditor", "pl", "notes", "bank_code"]]
        df = df.loc[(df["creditor"].notnull()) & (df["pl"].notnull())]
        return df

    def get_settled_purchase_invoices_new(self) -> List[Tuple[NewPurchaseInvoice, NewPurchaseLedgerPayment]]:
        return []

    def get_settled_sales_invoices(self):
        df = self.bank[["raw_id", "date", "amount", "debtor", "pl", "notes", "bank_code"]]
        df = df.loc[(df["debtor"].notnull()) & (df["pl"].notnull())]
        return df

    def get_unmatched_payments(self) -> List[NewPurchaseLedgerPayment]:
        df = self.bank.copy()
        df = df.loc[(df["creditor"].notnull()) & (df["pl"].isnull()) & (df["bs"].isnull())]
        df = df[["raw_id", "date", "amount", "creditor", "bank_code"]]
        payments = [NewPurchaseLedgerPayment(**x) for x in df.to_dict("record")]
        return payments

    def get_unmatched_receipts(self) -> List[NewSalesLedgerReceipt]:
        df = self.bank.copy()
        df = df.loc[(df["debtor"].notnull()) & (df["pl"].isnull()) & (df["bs"].isnull())]
        df = df[["raw_id", "date", "amount", "debtor", "bank_code"]]
        receipts = [NewSalesLedgerReceipt(**x) for x in df.to_dict("record")]
        return receipts

    @property
    def chart_of_accounts_config(self) -> List[NewNominal]:
        # TODO get accounts not listed in COA sheet, look at bank sheet too
        nominals = []
        for nominal in self.coa.to_dict("record"):
            nominal["name"] = nominal["nominal"]
            del nominal["nominal"]
            nominals.append(NewNominal(**nominal))
        return nominals


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
    bank_ledger = InMemoryBankLedgerTransactions()
    bank = BankLedger(ledger=bank_ledger)
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
    settled_invoices = parser.get_settled_purchase_invoices()
    settled_sales_invoices = parser.get_settled_sales_invoices()
    unmatched_payments = parser.get_unmatched_payments()
    unmatched_receipts = parser.get_unmatched_receipts()

    bank.ledger.add_transactions(bank_transactions)

    # Settled Purchase Ledger Invoices
    purchase_ledger.add_settled_transcations(settled_invoices)
    settled_pl_invoices = parser.get_settled_purchase_invoices_new()
    for invoice, payment in settled_pl_invoices:
        # Assuming invoice is one line, payment is one line
        invoice_trans_ids = purchase_ledger.add_invoices([invoice])
        payment_trans_ids = purchase_ledger.add_payments([payment])
        purchase_ledger.allocate_transactions(invoice_trans_ids.extend(payment_trans_ids))


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

    journals = inter_ledger_jnl_creator.create_bank_to_gl_journals(bank.ledger.list_transactions())
    for journal in journals:
        general.ledger.add_journal(journal)
        # TODO update bank_ledger that these have been added to gl

    # Reporting
    print("\nPublishing Report")
    print("Bank Ledger")
    report_writer.write_bank_ledger(bank.ledger)
    print("General Ledger")
    report_writer.write_general_ledger(general.ledger, general.chart_of_accounts)
    print("Purchase Ledger")
    report_writer.write_purchase_ledger(purchase_ledger)
    print("Sales Ledger")
    report_writer.write_sales_ledger(sales_ledger)

    # Validation
    print("Running validation checks")
    print("General Ledger sums to 0. Value:", general.ledger.balance, general.ledger.balance == 0)
    assert general.ledger.balance == 0
    # TODO each bank account sums to account on GL

    print("\n")
    print("Purchase Ledger Control Account agrees to Purchase Ledger")
    plca_value = general.ledger.balances["purchase_ledger_control_account"]
    print("PLCA value:", plca_value)
    purchase_ledger_balance = purchase_ledger.balance
    print("Purchase Ledger value:", purchase_ledger_balance)
    assert plca_value == purchase_ledger_balance
    print("\n")
    slca_value = general.ledger.balances["sales_ledger_control_account"]
    print("SLCA value:", slca_value)
    sales_ledger_balance = sales_ledger.balance
    print("Sales Ledger value:", sales_ledger_balance)
    assert slca_value == sales_ledger_balance

    # TODO validate num raw transactions vs num bank ledger transactions

    return


if __name__ == "__main__":
    main()
