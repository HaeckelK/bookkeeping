from reporting import IndexPage, StatementPage

main_page = IndexPage(id="root", title="Accounts Demo")
pl_index = IndexPage(id="pl_index", title="Purchase Ledger Reports")
gl_index = IndexPage(id="gl_index", title="General Ledger Reports")
pl_transactions = StatementPage(id="pl_transactions", title="PL Transactions")
pl_unallocated = StatementPage(id="pl_unallocated", title="PL Unallocated")

main_page.add_child(pl_index)
main_page.add_child(gl_index)
pl_index.add_child(pl_transactions)
pl_index.add_child(pl_unallocated)

print(main_page.child_links)
print(pl_unallocated.parent_link)