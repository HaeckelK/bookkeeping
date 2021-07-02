from django.db import models


class NominalAccount(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self) -> str:
        return self.name


class JournalLine(models.Model):
    nominal = models.ForeignKey(NominalAccount, on_delete=models.CASCADE)
    amount = models.IntegerField()

    def __str__(self) -> str:
        return f"{self.amount} - {self.nominal}"


class Journal(models.Model):
    description = models.CharField(max_length=250)
    period = models.IntegerField()
    # date_posted
    lines = models.ManyToManyField(JournalLine)


class GLTransactionLine(models.Model):
    pass


class PeriodBalance(models.Model):
    nominal = models.ForeignKey(NominalAccount, on_delete=models.CASCADE)
    period = models.IntegerField()
    amount = models.IntegerField()
    amount_cumulative = models.IntegerField()
    count_transactions = models.IntegerField()
