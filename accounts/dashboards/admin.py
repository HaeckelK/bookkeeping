from django.contrib import admin

from .models import NominalAccount, JournalLine, Journal, PeriodBalance

admin.site.register(NominalAccount)
admin.site.register(JournalLine)
admin.site.register(Journal)
admin.site.register(PeriodBalance)
