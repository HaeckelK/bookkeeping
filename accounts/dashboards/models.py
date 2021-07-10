from django.db import models


class NominalAccount(models.Model):
    EXPECTED_SIGN_CHOICES = [
        ("dr", "debit"),
        ("cr", "credit"),
    ]
    name = models.CharField(max_length=100)
    expected_sign = models.CharField(max_length=2, choices=EXPECTED_SIGN_CHOICES)
    is_control_account = models.BooleanField()
    is_bank_account = models.BooleanField()

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


class NominalTransaction(models.Model):
    transaction_id = models.IntegerField(unique=True)
    journal_id = models.IntegerField()
    date_transaction = models.DateField()
    period = models.IntegerField()
    nominal = models.ForeignKey(NominalAccount, on_delete=models.CASCADE)
    amount = models.IntegerField()
    description = models.CharField(max_length=500)

    @property
    def amount_display(self) -> str:
        decimal = self.amount / 100
        display = "{:,}".format(decimal)
        if len(display.split(".")[1]) == 1:
            display += "0"
        if self.amount < 0:
            display = display.replace("-", "")
            display = "(" + display + ")"
        return display
