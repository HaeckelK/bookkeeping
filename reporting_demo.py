from reporting import IndexPage, StatementPage, Report, MarkdownReportWriter, HTMLReportWriter

main_page = IndexPage(id="root", title="Demo Accounts Index")
pl_index = IndexPage(id="pl_index", title="Purchase Ledger Reports")
gl_index = IndexPage(id="gl_index", title="General Ledger Reports")
pl_transactions = StatementPage(id="pl_transactions", title="PL Transactions")
pl_unallocated = StatementPage(id="pl_unallocated", title="PL Unallocated")

main_page.add_child(pl_index)
main_page.add_child(gl_index)
pl_index.add_child(pl_transactions)
pl_index.add_child(pl_unallocated)

report = Report(root=main_page, title="My First Report", date_created=999)

writer = MarkdownReportWriter("data\\new_classes")
writer.write(report)

writer = HTMLReportWriter("data\\new_classes")
writer.write(report)
