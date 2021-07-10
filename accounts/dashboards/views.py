from django.shortcuts import render, HttpResponse

from .models import PeriodBalance, NominalTransaction, NominalAccount


def trial_balance(request):
    period = request.GET.get("period", -1)
    # TODO get the boolean variable in fewer lines
    cumulative_raw = request.GET.get("cumulative", 0)
    if cumulative_raw == "1":
        cumulative = True
    else:
        cumulative = False

    balances = PeriodBalance.objects.filter(period=period)
    context = {"balances": balances, "period": period, "cumulative": cumulative}
    return render(request, "trial_balance.html", context)


def nominal_transactions(request):
    period_from = int(request.GET.get("period_start", 1))
    period_to = int(request.GET.get("period_end", 12))
    nominal_names = request.GET.get("nominals", "").split(",")
    journal_ids = request.GET.get("journals", "").split(",")

    query_params = f"&period_start={period_from}&period_end={period_to}"

    if nominal_names == [""]:
        nominal_names = NominalAccount.objects.values_list("name", flat=True)

    if journal_ids == [""]:
        journal_ids = NominalTransaction.objects.values_list("journal_id", flat=True)

    transactions = NominalTransaction.objects.filter(
        nominal__name__in=nominal_names, journal_id__in=journal_ids, period__gte=period_from, period__lte=period_to
    )

    link_next, link_previous = "", ""
    if len(nominal_names) == 1:
        nominal_account = NominalAccount.objects.get(name=nominal_names[0])
        try:
            next_name = NominalAccount.objects.get(pk=nominal_account.pk + 1)
        except NominalAccount.DoesNotExist:
            pass
        else:
            link_next = f"/nominal_transactions/?nominals={next_name.name}{query_params}"
        try:
            previous_name = NominalAccount.objects.get(pk=nominal_account.pk - 1)
        except NominalAccount.DoesNotExist:
            pass
        else:
            link_previous = f"/nominal_transactions/?nominals={previous_name.name}{query_params}"

    context = {
        "transactions": transactions,
        "period_from": period_from,
        "period_to": period_to,
        "link_next": link_next,
        "link_previous": link_previous,
        "query_params": query_params,
    }
    return render(request, "nominal_transactions.html", context)
