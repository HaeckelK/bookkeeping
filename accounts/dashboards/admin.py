from django.contrib import admin

from .models import NominalAccount, JournalLine, Journal, PeriodBalance, NominalTransactions

admin.site.register(NominalAccount)
admin.site.register(JournalLine)
admin.site.register(Journal)
admin.site.register(PeriodBalance)
admin.site.register(NominalTransactions)
