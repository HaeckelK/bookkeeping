from dataclasses import dataclass, asdict
from typing import List, Tuple
import os
import re

import pandas as pd

from general import (
    GLJournal,
    GLJournalLine,
    GeneralLedgerTransactions,
    GeneralLedger,
    InMemoryChartOfAccounts,
    NewNominal,
)
from bank import BankTransaction, InMemoryBankLedgerTransactions, RawBankTransaction, BankLedger
from purchases import (
    NewPurchaseInvoice,
    PurchaseInvoice,
    NewPurchaseInvoiceLine,
    PurchaseLedger,
    NewPurchaseLedgerPayment,
)
from sales import SalesLedger, NewSalesLedgerReceipt, SalesInvoiceLine, SalesInvoice
from reporting import HTMLRawReportWriter
from utils import convert_date_string_to_period


@dataclass
class EntityData:
    name: str
    cashbook: str


class ExcelSourceDataLoader:
    def __init__(
        self,
        filename: str,
        bank_sheet: str,
        coa_sheet: str,
        si_headers_sheet: str,
        si_lines_sheet: str,
        gl_jnl_headers_sheet: str,
        gl_jnl_lines_sheet: str,
    ) -> None:
        self.filename = filename
        self.bank_sheet = bank_sheet
        self.coa_sheet = coa_sheet
        self.si_headers_sheet = si_headers_sheet
        self.si_lines_sheet = si_lines_sheet
        self.gl_jnl_headers_sheet = gl_jnl_headers_sheet
        self.gl_jnl_lines_sheet = gl_jnl_lines_sheet

        self.bank = None
        self.coa = None
        self.sales_invoice_headers = None
        self.sales_invoice_lines = None
        self.gl_journal_headers = None
        self.gl_journal_lines = None
        return

    def load(self):
        print("Loading Bank Sheet")
        self.load_bank()
        print("Loading COA Sheet")
        self.load_coa()
        print("Loading Sales Invoice Headers")
        self.load_sales_invoice_headers()
        print("Loading Sales Invoice Lines")
        self.load_sales_invoice_lines()
        print("Loading GL Journal Headers")
        self.load_gl_journal_headers()
        print("Loading GL Journal Lines")
        self.load_gl_journal_lines()
        return

    def load_bank(self):
        # TODO set all column names to lower
        df = pd.read_excel(self.filename, sheet_name=self.bank_sheet, index_col=None)
        df["period"] = df["date"].apply(convert_date_string_to_period)
        df["amount"] = df["amount"] * 100
        df = df.astype({"amount": "int32"})
        df.insert(0, "raw_id", range(0, 0 + len(df)))
        self.bank = df
        return

    def load_coa(self):
        df = pd.read_excel(self.filename, sheet_name=self.coa_sheet, index_col=None)
        df["control_account"] = df["control_account"].map({"y": True, "n": False})
        df["bank_account"] = df["bank_account"].map({"y": True, "n": False})
        self.coa = df
        return

    def load_sales_invoice_headers(self):
        df = pd.read_excel(self.filename, sheet_name=self.si_headers_sheet, index_col=None)
        df["period"] = df["date"].apply(convert_date_string_to_period)
        self.sales_invoice_headers = df
        return

    def load_sales_invoice_lines(self):
        df = pd.read_excel(self.filename, sheet_name=self.si_lines_sheet, index_col=None)
        df["period"] = df["transaction_date"].apply(convert_date_string_to_period)
        df["amount"] = df["amount"] * 100
        df = df.astype({"amount": "int32"})
        df.insert(0, "line_id", range(0, 0 + len(df)))
        self.sales_invoice_lines = df
        return

    def load_gl_journal_headers(self):
        df = pd.read_excel(self.filename, sheet_name=self.gl_jnl_headers_sheet, index_col=None)
        df["period"] = df["date"].apply(convert_date_string_to_period)
        self.gl_journal_headers = df
        return

    def load_gl_journal_lines(self):
        df = pd.read_excel(self.filename, sheet_name=self.gl_jnl_lines_sheet, index_col=None)
        df["period"] = df["transaction_date"].apply(convert_date_string_to_period)
        df["amount"] = df["amount"] * 100
        df = df.astype({"amount": "int32"})
        df.insert(0, "line_id", range(0, 0 + len(df)))
        self.gl_journal_lines = df
        return


class SourceDataParser:
    def register_source_data(
        self, bank, coa, sales_invoice_headers, sales_invoice_lines, gl_journal_headers, gl_journal_lines
    ) -> None:
        self.bank = bank
        self.coa = coa
        self.sales_invoice_headers = sales_invoice_headers
        self.sales_invoice_lines = sales_invoice_lines
        self.gl_journal_headers = gl_journal_headers
        self.gl_journal_lines = gl_journal_lines
        self.extend_coa()
        return

    def extend_coa(self) -> None:
        """self.coa may not be complete. Add any nominals seen in other sheets."""
        additional_nominals = []
        existing_nominals = self.coa["nominal"].unique()
        for field in ("bs", "pl"):
            for bank_nominal in self.bank[field].unique():
                if isinstance(bank_nominal, str) is False:
                    continue
                if bank_nominal in existing_nominals:
                    continue
                additional_nominals.append(
                    {
                        "nominal": bank_nominal,
                        "statement": field.lower(),
                        "expected_sign": "dr",
                        "control_account": False,
                        "bank_account": False,
                        "heading": "NOMINAL DETAILS MISSING",
                    }
                )
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

        matched = pd.DataFrame.from_dict(matched_dict, orient="index", columns=["matched_account", "matched_type"])

        df = self.bank[["date", "transaction_type", "description", "amount", "transfer_type", "raw_id", "bank_code"]]
        df = df.join(matched)
        lines = []
        for transaction in df.to_dict("records"):
            lines.append(RawBankTransaction(**transaction))
        return lines

    def get_settled_purchase_invoices(self) -> List[Tuple[NewPurchaseInvoice, NewPurchaseLedgerPayment]]:
        df = self.bank[["raw_id", "date", "amount", "creditor", "pl", "notes", "bank_code"]]
        df = df.loc[(df["creditor"].notnull()) & (df["pl"].notnull())]

        items = []
        for line in df.to_dict("record"):
            pl_lines = [
                NewPurchaseInvoiceLine(
                    nominal=line["pl"],
                    description=line["notes"],
                    amount=-line["amount"],
                    transaction_date=line["date"],
                    raw_id=line["raw_id"],
                )
            ]
            item = (
                NewPurchaseInvoice(creditor=line["creditor"], lines=pl_lines),
                NewPurchaseLedgerPayment(
                    raw_id=line["raw_id"],
                    date=line["date"],
                    amount=line["amount"],
                    creditor=line["creditor"],
                    bank_code=line["bank_code"],
                ),
            )
            items.append(item)
        return items

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
    def sales_invoices(self) -> List[SalesInvoice]:
        headers = self.sales_invoice_headers.to_dict("record")
        all_lines = self.sales_invoice_lines.to_dict("record")

        invoices = []
        for header in headers:
            raw_lines = [x for x in all_lines if x["header_id"] == header["id"]]
            lines = []
            for raw_line in raw_lines:
                lines.append(
                    SalesInvoiceLine(
                        raw_line["nominal"], raw_line["description"], raw_line["amount"], raw_line["transaction_date"]
                    )
                )
            invoice = SalesInvoice(header["debtor"], lines=lines)
            invoices.append(invoice)
        return invoices

    @property
    def gl_journals(self) -> List[GLJournal]:
        headers = self.gl_journal_headers.to_dict("record")
        all_lines = self.gl_journal_lines.to_dict("record")

        invoices = []
        for header in headers:
            raw_lines = [x for x in all_lines if x["header_id"] == header["id"]]
            lines = []
            for raw_line in raw_lines:
                lines.append(
                    GLJournalLine(
                        raw_line["nominal"], raw_line["description"], raw_line["amount"], raw_line["transaction_date"]
                    )
                )
            invoice = GLJournal(header["jnl_type"], lines=lines)
            invoices.append(invoice)
        return invoices

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
    def create_pl_to_gl_journals(self, invoices: List[PurchaseInvoice]) -> List[Tuple[GLJournal, List[int]]]:
        output = []
        total = sum(x.total for x in invoices)
        transaction_date = max(line.transaction_date for invoice in invoices for line in invoice.lines)
        transaction_ids = []
        gl_lines = [
            GLJournalLine(
                nominal="purchase_ledger_control_account",
                description="PL dispersal to GL",
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
                transaction_ids.extend(invoice.transaction_ids)

        journal = GLJournal(jnl_type="pi", lines=gl_lines)
        output.append((journal, transaction_ids))
        return output

    # TODO DRY see create_pl_to_gl_journals
    def create_sl_to_gl_journals(self, invoices: List[PurchaseInvoice]) -> List[GLJournal]:
        total = sum(x.total for x in invoices)
        transaction_date = max(line.transaction_date for invoice in invoices for line in invoice.lines)
        gl_lines = [
            GLJournalLine(
                nominal="sales_ledger_control_account",
                description="SL dispersal to GL",
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
        dates_df = (
            df[["bank_code", "matched_type", "date"]].copy().groupby(["bank_code", "matched_type"]).max().reset_index()
        )
        df = df[["bank_code", "matched_type", "amount"]].groupby(["bank_code", "matched_type"]).sum().reset_index()

        df = pd.merge(df, dates_df, how="left", on=["bank_code", "matched_type"]).reset_index()

        lookup = {
            "creditor": "purchase_ledger_control_account",
            "debtor": "sales_ledger_control_account",
            "bs": "bank_contra",
        }
        journals = []
        for line in df.to_dict("record"):
            bank_code = line["bank_code"]
            amount = -line["amount"]
            gl_account = lookup[line["matched_type"]]
            transcation_date = line["date"]
            description = f"{bank_code} to {gl_account}"

            gl_lines = [
                GLJournalLine(
                    nominal=bank_code,
                    description=description,
                    amount=amount,
                    transaction_date=transcation_date,
                ),
                GLJournalLine(
                    nominal=gl_account,
                    description=description,
                    amount=-amount,
                    transaction_date=transcation_date,
                ),
            ]
            journal = GLJournal(jnl_type="bank", lines=gl_lines)
            journals.append(journal)
        return journals


def filter_by_period(df: pd.DataFrame, period: int) -> pd.DataFrame:
    data = df.copy()
    data = data.loc[(df["period"] == period)]
    return data


def entity_loop(filename: str, entity_name: str):
    data_loader = ExcelSourceDataLoader(
        filename=filename,
        bank_sheet="bank",
        coa_sheet="coa",
        si_headers_sheet="sales_invoice_headers",
        si_lines_sheet="sales_invoice_lines",
        gl_jnl_headers_sheet="gl_journal_headers",
        gl_jnl_lines_sheet="gl_journal_lines",
    )
    parser = SourceDataParser()
    bank_ledger = InMemoryBankLedgerTransactions()
    bank = BankLedger(ledger=bank_ledger)
    purchase_ledger = PurchaseLedger()
    sales_ledger = SalesLedger()
    general_ledger = GeneralLedgerTransactions()
    general = GeneralLedger(ledger=general_ledger, chart_of_accounts=InMemoryChartOfAccounts())
    inter_ledger_jnl_creator = InterLedgerJournalCreator()
    report_writer = HTMLRawReportWriter(path="data/html", entity_name=entity_name)

    print("Bookkeeping Demo")
    print("Load source excel")
    data_loader.load()

    for period in range(1, 13):
        print(f"\nCurrent Period: {period}")
        period_bank = filter_by_period(data_loader.bank, period)
        period_sales_invoice_headers = filter_by_period(data_loader.sales_invoice_headers, period)
        period_sales_invoice_lines = filter_by_period(data_loader.sales_invoice_lines, period)
        period_gl_journal_headers = filter_by_period(data_loader.gl_journal_headers, period)
        period_gl_journal_lines = filter_by_period(data_loader.gl_journal_lines, period)

        parser.register_source_data(
            bank=period_bank,
            coa=data_loader.coa,
            sales_invoice_headers=period_sales_invoice_headers,
            sales_invoice_lines=period_sales_invoice_lines,
            gl_journal_headers=period_gl_journal_headers,
            gl_journal_lines=period_gl_journal_lines,
        )

        # Setup financials config
        nominals = parser.chart_of_accounts_config
        print("\nAdding nominal accounts to COA")
        for nominal in nominals:
            # TODO COA should have a method to check
            if nominal.name in [x.name for x in general.chart_of_accounts.nominals]:
                continue
            print(f"..{nominal.name}")
            general.chart_of_accounts.add_nominal(nominal)

        bank_transactions = parser.get_bank_transactions()
        gl_journals = parser.gl_journals
        settled_sales_invoices = parser.get_settled_sales_invoices()
        sales_invoices = parser.sales_invoices
        unmatched_payments = parser.get_unmatched_payments()
        unmatched_receipts = parser.get_unmatched_receipts()

        if bank_transactions:
            bank.ledger.add_transactions(bank_transactions)

        # Settled Purchase Ledger Invoices
        print("\nAdding settled invoices to Purchase Ledger")
        settled_pl_invoices = parser.get_settled_purchase_invoices()
        for invoice, payment in settled_pl_invoices:
            # Assuming invoice is one line, payment is one line
            print("..Adding invoices")
            invoice_trans_ids = purchase_ledger.add_invoices([invoice])
            print("....invoice_trans_ids", invoice_trans_ids)
            print("..Adding corresponding payments")
            payment_trans_ids = purchase_ledger.add_payments([payment])
            print("....payment_trans_ids", payment_trans_ids)
            allocation_ids = invoice_trans_ids + payment_trans_ids
            print("..Allocating transactions", allocation_ids)
            purchase_ledger.allocate_transactions(allocation_ids)

        print("\nAdding unmatched payments to Purchase Ledger")
        if unmatched_payments:
            ids = purchase_ledger.add_payments(unmatched_payments)
            print("..Purchase ledger ids:", ids)

        sales_ledger.add_settled_transcations(settled_sales_invoices)
        if unmatched_receipts:
            sales_ledger.add_receipts(unmatched_receipts)
        print("Adding Sales Ledger Invoices")
        sales_ledger.add_invoices(sales_invoices)

        print("\nDispersing Purchase Ledger invoice to General Ledger")
        # TODO this needs to return List[Tuple[journals, purchase invoice ID]]
        # Hmm bigger issue here is that there is no link from Purchase Invoice to PL transaction id.
        pl_unposted_invoices = purchase_ledger.get_unposted_invoices()
        if pl_unposted_invoices:
            journals = inter_ledger_jnl_creator.create_pl_to_gl_journals(pl_unposted_invoices)
            for journal, transaction_ids in journals:
                print(f"..{journal.jnl_type}: {journal.total}")
                ids = general.ledger.add_journal(journal)
                print("....General ledger ids:", ids)
                print("..marking extracted in Purchase Ledger", transaction_ids)
                purchase_ledger.mark_extracted_to_gl(transaction_ids)

        print("\nDispersing Sales Ledger invoice to General Ledger")
        pl_unposted_invoices = sales_ledger.get_unposted_invoices()
        if pl_unposted_invoices:
            journals = inter_ledger_jnl_creator.create_sl_to_gl_journals(pl_unposted_invoices)
            for journal in journals:
                print(f"..{journal.jnl_type}: {journal.total}")
                ids = general.ledger.add_journal(journal)
                print("....General ledger ids:", ids)
                print("..marking extracted in Purchase Ledger", ids)
                # sales_ledger.mark_extracted_to_gl(ids)
                # Hack - needs to only mark items that were just posted
                sales_ledger.mark_all_posted()

        print("\nDispersing Bank Ledger to General Ledger")
        # TODO maybe this should only be bank to PL and SL + direct to GL, then from PL and SL to GL
        bank_transactions = bank.ledger.get_unposted_transactions()
        if bank_transactions:
            journals = inter_ledger_jnl_creator.create_bank_to_gl_journals(bank_transactions)
            for journal in journals:
                general.ledger.add_journal(journal)
                # Hack - needs to only mark items that were just posted
                bank.ledger.mark_all_posted()

        print("\nPosting GL Journals")
        for journal in gl_journals:
            general.ledger.add_journal(journal)

        # Validation
        print("Running validation checks")
        print("General Ledger sums to 0. Value:", general.ledger.balance, general.ledger.balance == 0)
        assert general.ledger.balance == 0
        # TODO each bank account sums to account on GL

        print("\n")
        print("Purchase Ledger Control Account agrees to Purchase Ledger")
        try:
            plca_value = general.ledger.balances["purchase_ledger_control_account"]
        except KeyError:
            plca_value = 0
        print("PLCA value:", plca_value)
        purchase_ledger_balance = purchase_ledger.balance
        print("Purchase Ledger value:", purchase_ledger_balance)
        assert plca_value == purchase_ledger_balance
        print("\n")
        try:
            slca_value = general.ledger.balances["sales_ledger_control_account"]
        except KeyError:
            slca_value = 0
        print("SLCA value:", slca_value)
        sales_ledger_balance = sales_ledger.balance
        print("Sales Ledger value:", sales_ledger_balance)
        assert slca_value == sales_ledger_balance

        # TODO validate num raw transactions vs num bank ledger transactions
    # Reporting
    print("\nPublishing Report")
    print("..Bank Ledger")
    report_writer.write_bank_ledger(bank.ledger)
    print("..General Ledger")
    report_writer.write_general_ledger(general.ledger, general.chart_of_accounts)
    print("..Purchase Ledger")
    report_writer.write_purchase_ledger(purchase_ledger)
    print("..Sales Ledger")
    report_writer.write_sales_ledger(sales_ledger)
    return


def get_entities_data(folder: str) -> List[EntityData]:
    print("Identifying entity cashbooks")
    entities = []
    for cashbook in os.listdir(folder):
        name = re.findall(r"cashbook_(.*).xlsx", cashbook)[0]
        filename = os.path.join(folder, cashbook)
        entity = EntityData(name=name, cashbook=filename)
        entities.append(entity)
    return entities


def main():
    entities_data = get_entities_data("data/cashbooks")
    for entity in entities_data:
        print(f"\nProcessing Entity: {entity.name}")
        entity_loop(filename=entity.cashbook, entity_name=entity.name)
    return


if __name__ == "__main__":
    main()
