from abc import ABC, abstractmethod
from dataclasses import asdict

import pandas as pd

from bank import BankLedger


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
