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


# TODO ReportWriter probably doesn't need to be ABC, its the Buffer class that should be split out?
# TODO this should really be a report tree producer and writer which takes a completed tree
# tricky bit is the creation of internal links etc.
# TODO Page should just be extended to contain the information held in Buffer.
class ReportWriter(ABC):
    def write(self, report: Report) -> None:
        print(f"{type(self)} begin writing")
        self.write_page(report.root)
        return

    @abstractmethod
    def get_link_to_page(self, page_id: str) -> str:
        """Return internal link used between output pages created by ReportWriter"""

    def write_page(self, page: Page) -> None:
        print("Processing", page.title)
        self.current_page = page
        self.create_page_output(page)
        for child in page.children:
            self.write_page(child)
        return

    def create_page_output(self, page: Page) -> None:
        self.reset_buffer()

        if isinstance(page.parent_link, NullTextLink) is False:
            self.buffer_add_parent_link(
                TextLink(page.parent_link.display, self.get_link_to_page(page.parent_link.link_page_id))
            )

        self.buffer_add_title(page.title)

        child_links = []
        for child in page.children:
            child_links.append(TextLink(child.title, self.get_link_to_page(child.id)))
        self.buffer_add_child_links(child_links)

        self.buffer_save()
        return

    @abstractmethod
    def reset_buffer(self) -> None:
        """Return buffer to blank state ready for new page information."""

    @abstractmethod
    def buffer_add_title(self, title: str) -> None:
        """Add page title to buffer."""

    @abstractmethod
    def buffer_add_child_links(self, links: List[TextLink]) -> None:
        """Add list of TextLink to child pages to buffer."""

    @abstractmethod
    def buffer_add_parent_link(self, link: TextLink) -> None:
        """Add link to parent page to buffer."""

    @abstractmethod
    def buffer_save(self) -> None:
        """Save produced page in final format and medium."""


class MarkdownReportWriter(ReportWriter):
    def __init__(self, path: str) -> None:
        self.path = path
        return

    def get_link_to_page(self, page_id: str) -> str:
        # TODO does this need to be based off parent link?
        return os.path.join(page_id + ".md")

    def reset_buffer(self) -> None:
        """Return buffer to blank state ready for new page information."""
        self.buffer = ""
        return

    def buffer_add_title(self, title: str) -> None:
        """Add page title to buffer."""
        self.buffer += f"\n# {title}"
        return

    def buffer_add_child_links(self, links: List[TextLink]) -> None:
        """Add list of TextLink to child pages to buffer."""
        for link in links:
            self.buffer += f"\n- [{link.display}]({link.link_page_id})"
        return

    def buffer_add_parent_link(self, link: TextLink) -> None:
        """Add link to parent page to buffer."""
        self.buffer += f"[{link.display}]({link.link_page_id})"
        return

    def buffer_save(self) -> None:
        """Save produced page in final format and medium."""
        filename = os.path.join(self.path, self.current_page.id + ".md")
        with open(filename, "w") as f:
            f.write(self.buffer)
        return


class HTMLReportWriter(ReportWriter):
    def __init__(self, path: str) -> None:
        self.path = path
        return

    def get_link_to_page(self, page_id: str) -> str:
        # TODO does this need to be based off parent link?
        return os.path.join(page_id + ".html")

    def reset_buffer(self) -> None:
        """Return buffer to blank state ready for new page information."""
        self.buffer = {}
        return

    def buffer_add_title(self, title: str) -> None:
        """Add page title to buffer."""
        self.buffer["title"] = title
        return

    def buffer_add_child_links(self, links: List[TextLink]) -> None:
        """Add list of TextLink to child pages to buffer."""
        self.buffer["child_links"] = links
        return

    def buffer_add_parent_link(self, link: TextLink) -> None:
        """Add link to parent page to buffer."""
        self.buffer["parent_link"] = link
        return

    def buffer_save(self) -> None:
        """Save produced page in final format and medium."""
        filename = os.path.join(self.path, self.current_page.id + ".html")
        html = """<html>
  <head>
    <title>{TITLE}</title>
  </head>
  <body>
    {PARENT_LINK}
    <h1>{TITLE}</h1>
    {CHILD_LINKS}
  </body>
</html>"""
        html = html.replace("{TITLE}", self.buffer["title"])
        try:
            parent = self.buffer["parent_link"]
            parent_link = f'<a href="{parent.link_page_id}">{parent.display}</a>'
        except KeyError:
            parent_link = ""
        if self.buffer["child_links"]:
            child_links = "<ul>"
            for link in self.buffer["child_links"]:
                child_links += "\n<li>"
                # TODO .html should not already be coming through here?
                child_links += f'<a href="{self.get_link_to_page(link.link_page_id).replace(".html.html", ".html")}">{link.display}</a>'  # noqa: E501
                child_links += "</li>"
            child_links += "\n</ul>"
        else:
            child_links = ""
        html = html.replace("{PARENT_LINK}", parent_link)
        html = html.replace("{CHILD_LINKS}", child_links)
        with open(filename, "w") as f:
            f.write(html)
        return


# TODO write methods for all ledgers
class RawReportWriter(ABC):
    @abstractmethod
    def write_bank_ledger(self, ledger: BankLedgerTransactions):
        """"""

    @abstractmethod
    def write_general_ledger(self, ledger: GeneralLedgerTransactions):
        """"""


class CSVRawReportWriter(RawReportWriter):
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


class HTMLRawReportWriter(RawReportWriter):
    def __init__(self, path: str, entity_name: str) -> None:
        self.entity_name = entity_name
        self.path = os.path.join(path, entity_name)
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
        # classes and passed into new methods of RawReportWriter
        coa_df = pd.DataFrame([asdict(x) for x in coa.nominals])
        coa_df = coa_df.rename(columns={"name": "nominal"})
        balances = df[["nominal", "amount"]].groupby(["nominal"]).sum()
        balances = balances.join(coa_df.set_index("nominal"), on="nominal")
        balances = balances.reset_index()[["statement", "heading", "nominal", "amount"]]
        balances["nominal"] = balances["nominal"] + "_NOMINAL"
        balances.sort_values(by=["statement", "heading", "nominal"]).to_html(
            os.path.join(self.path, "trial_balance.html"), index=False
        )

        balances_period = df[["nominal", "period", "amount"]].groupby(["nominal", "period"]).sum()
        balances_period = (
            balances_period.reset_index().pivot(index="nominal", columns="period", values="amount").reset_index()
        )
        balances_period = balances_period.join(
            coa_df[["statement", "heading", "nominal"]].set_index("nominal"), on="nominal"
        )
        cols = ["statement", "heading", "nominal"] + [
            col for col in balances_period if col not in ["statement", "heading", "nominal"]
        ]
        balances_period = balances_period.reset_index()[cols]
        balances_period = balances_period.fillna(0)
        balances_period["nominal"] = balances_period["nominal"] + "_NOMINAL"
        balances_period.sort_values(by=["statement", "heading", "nominal"]).to_html(
            os.path.join(self.path, "trial_balance_period_movement.html"), index=False
        )

        nominals = df["nominal"].unique()
        for nominal in nominals:
            nominal_df = df.loc[(df["nominal"] == nominal)]
            nominal_df.to_html(os.path.join(self.nominals_path, f"{nominal}.html"), index=False)

        # Hack to add links to prebuilt to_html table
        for basename in ("trial_balance.html", "trial_balance_period_movement.html"):
            filename = os.path.join(self.path, basename)
            with open(filename, "r") as f:
                html = f.read().splitlines()

            regex = r"<td>(\D+)<\/td>"
            new_html = []
            for line in html:
                matches = re.findall(regex, line)
                if matches and "_NOMINAL" in line:
                    nominal = matches[0].replace("_NOMINAL", "")
                    new_line = line.replace(
                        nominal + "_NOMINAL",
                        f'<a href="/{self.entity_name}/nominal_transactions/{nominal}.html">{nominal}</a>',
                    )
                    new_html.append(new_line)
                else:
                    new_html.append(line)

            with open(filename, "w") as f:
                f.write("\n".join(new_html))
        return
