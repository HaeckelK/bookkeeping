from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
import os
import re
from typing import List

import pandas as pd

from bank import BankLedgerTransactions
from general import ChartOfAccounts, GeneralLedgerTransactions


@dataclass
class TextLink:
    display: str
    link_page_id: str


@dataclass
class NullTextLink(TextLink):
    display: str = "NA"
    link_page_id: str = "NA"


class Page:
    def __init__(self, id: str, title: str) -> None:
        self.id = id
        self.title = title
        self.children: List[Page] = []
        self.parent_link = NullTextLink()
        return

    def add_child(self, page) -> None:
        page.parent_link = TextLink(self.title, self.id)
        self.children.append(page)
        return

    @property
    def child_links(self) -> List[TextLink]:
        return [TextLink(display=x.title, link_page_id=x.id) for x in self.children]


class IndexPage(Page):
    """List of links to other pages."""


class StatementPage(Page):
    """Presents tabular data."""


@dataclass
class Report:
    root: Page
    title: str
    date_created: int


class MarkdownReportWriter:
    def __init__(self, path: str) -> None:
        self.path = path
        return

    def write(self, report: Report) -> None:
        print("MarkdownReportWriter begin writing")
        self.write_page(report.root)
        return

    def write_page(self, page: Page) -> None:
        print("Processing", page.title)
        filename = os.path.join(self.path, page.id + ".md")
        with open(filename, "w") as f:
            if isinstance(page.parent_link, NullTextLink) is False:
                f.write(f"[{page.parent_link.display}]({self.get_link_to_page(page.parent_link.link_page_id)})")
            f.write(f"\n# {page.title}")
            for child in page.children:
                f.write(f"\n- [{child.title}]({self.get_link_to_page(child.id)})")

        for child in page.children:
            self.write_page(child)
        return

    def get_link_to_page(self, page_id: str) -> str:
        # TODO does this need to be based off parent link?
        return os.path.join(page_id + ".md")
 

# TODO write methods for all ledgers
class ReportWriter(ABC):
    @abstractmethod
    def write_bank_ledger(self, ledger: BankLedgerTransactions):
        """"""

    @abstractmethod
    def write_general_ledger(self, ledger: GeneralLedgerTransactions):
        """"""


class CSVReportWriter(ReportWriter):
    def write_bank_ledger(self, ledger: BankLedgerTransactions):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df = df[ledger.columns]
        df.to_csv("ledger_transactions/bank_ledger.csv", index=False)
        return

    def write_general_ledger(self, ledger: GeneralLedgerTransactions):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df = df[ledger.columns]
        df.to_csv("data/general_ledger.csv", index=False)
        return


class HTMLReportWriter(ReportWriter):
    def __init__(self, path: str) -> None:
        self.path = path
        self.ledgers_path = os.path.join(self.path, "ledger_transactions")
        self.nominals_path = os.path.join(self.path, "nominal_transactions")

        if os.path.exists(self.ledgers_path) is False:
            os.makedirs(self.ledgers_path)

        if os.path.exists(self.nominals_path) is False:
            os.makedirs(self.nominals_path)

        return

    def write_bank_ledger(self, ledger: BankLedgerTransactions):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df = df[ledger.columns]
        df.to_html(os.path.join(self.ledgers_path, "bank_ledger.html"), index=False)
        return

    # TODO data type PurchaseLedger
    # TODO don't access df directly
    def write_purchase_ledger(self, ledger):
        df = ledger.df
        df.to_html(os.path.join(self.ledgers_path, "purchase_ledger.html"), index=False)
        return

    # TODO data type PurchaseLedger
    # TODO don't access df directly
    def write_sales_ledger(self, ledger):
        df = ledger.df
        df.to_html(os.path.join(self.ledgers_path, "sales_ledger.html"), index=False)
        return

    def write_general_ledger(self, ledger: GeneralLedgerTransactions, coa: ChartOfAccounts):
        transactions = ledger.list_transactions()
        df = pd.DataFrame([asdict(x) for x in transactions])
        df = df[ledger.columns]
        df.to_html(os.path.join(self.ledgers_path, "general_ledger.html"), index=False)
        df["amount"] = df["amount"] / 100

        # TODO all df manipulations below here should be being created by Statement and Nominal producing
        # classes and passed into new methods of ReportWriter
        coa_df = pd.DataFrame([asdict(x) for x in coa.nominals])
        coa_df = coa_df.rename(columns={"name": "nominal"})
        balances = df[["nominal", "amount"]].groupby(["nominal"]).sum()
        balances = balances.join(coa_df.set_index("nominal"), on="nominal")
        balances = balances.reset_index()[["statement", "heading", "nominal", "amount"]]
        balances["nominal"] = balances["nominal"] + "_NOMINAL"
        balances.sort_values(by=["statement", "heading", "nominal"]).to_html(
            os.path.join(self.path, "trial_balance.html"), index=False
        )

        nominals = df["nominal"].unique()
        for nominal in nominals:
            nominal_df = df.loc[(df["nominal"] == nominal)]
            nominal_df.to_html(os.path.join(self.nominals_path, f"{nominal}.html"), index=False)

        # Hack to add links to prebuilt to_html table
        filename = os.path.join(self.path, "trial_balance.html")
        with open(filename, "r") as f:
            html = f.read().splitlines()

        regex = r"<td>(\D+)<\/td>"
        new_html = []
        for line in html:
            matches = re.findall(regex, line)
            if matches and "_NOMINAL" in line:
                nominal = matches[0].replace("_NOMINAL", "")
                new_line = line.replace(
                    nominal + "_NOMINAL", f'<a href="/nominal_transactions/{nominal}.html">{nominal}</a>'
                )
                new_html.append(new_line)
            else:
                new_html.append(line)

        with open(filename, "w") as f:
            f.write("\n".join(new_html))
        return
