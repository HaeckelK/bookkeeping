from abc import ABC, abstractmethod

import pandas as pd


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

    def get_unmatched_creditor_payments(self):
        df = self.df.copy()
        df = df.loc[(df['Creditor'].notnull()) & (df['PL'].isnull()) & (df['BS'].isnull())]
        df = df[['raw_id', 'Date', 'Amount', 'Creditor', 'Notes', 'Bank']]
        return df


class Ledger(ABC):
    @abstractmethod
    def get_next_batch_id(self) -> int:
        """Return next available batch id."""


class PandasLedger(Ledger):
    def get_next_batch_id(self) -> int:
        try:
            next_id = int(self.df["batch_id"].max())
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


def main():
    data_loader = SourceDataLoader()
    parser = SourceDataParser()
    bank_ledger = BankLedger()
    purchase_ledger = PurchaseLedger()

    print("Bookkeeping Demo")
    print("Load source excel")
    raw_data = data_loader.load("data/cashbook.xlsx", "bank")
    parser.register_source_data(raw_data)

    bank_transactions = parser.get_bank_transactions()
    settled_invoices = parser.get_settled_invoices()
    unmatched_creditor_payments = parser.get_unmatched_creditor_payments()

    # TODO do this in batches
    bank_ledger.add_transactions(bank_transactions, bank_code="nwa_ca")

    purchase_ledger.add_settled_transcations(settled_invoices, bank_code="nwa_ca")
    purchase_ledger.add_payments(unmatched_creditor_payments)

    bank_ledger.df.to_csv("data/bank_ledger.csv", index=False)
    purchase_ledger.df.to_csv("data/purchase_ledger.csv", index=False)
    return


if __name__ == "__main__":
    main()
