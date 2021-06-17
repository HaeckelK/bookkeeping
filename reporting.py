from abc import ABC, abstractmethod
from dataclasses import asdict

import pandas as pd

from bank import BankLedger
from general import GeneralLedger


class ReportWriter(ABC):
    @abstractmethod
    def write_bank_ledger(self, ledger: BankLedger):
        """"""

    @abstractmethod
    def write_general_ledger(self, ledger: GeneralLedger):
        """"""


class CSVReportWriter(ReportWriter):
    def write_bank_ledger(self, ledger: BankLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_csv("data/bank_ledger.csv", index=False)
        return

    def write_general_ledger(self, ledger: GeneralLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_csv("data/general_ledger.csv", index=False)
        return


class HTMLReportWriter(ReportWriter):
    def write_bank_ledger(self, ledger: BankLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_html("data/html/bank_ledger.html", index=False)
        return

    def write_general_ledger(self, ledger: GeneralLedger):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df.to_html("data/html/general_ledger.html", index=False)
        return
