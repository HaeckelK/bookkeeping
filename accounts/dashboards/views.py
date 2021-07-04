from django.shortcuts import render, HttpResponse

from .models import PeriodBalance, NominalTransaction


def trial_balance(request):
    period = request.GET.get('period',-1)
    # TODO get the boolean variable in fewer lines
    cumulative_raw = request.GET.get('cumulative', 0)
    if cumulative_raw == "1":
        cumulative = True
    else:
        cumulative = False

    balances = PeriodBalance.objects.filter(period=period)
    context = {"balances": balances,
               "period": period,
               "cumulative": cumulative}
    return render(request, "trial_balance.html", context)


def nominal_transactions(request):
    period_from = int(request.GET.get("period_start", 1))
    period_to = int(request.GET.get("period_end", 12))
    nominal_names = request.GET.get("nominals", "").split(",")

    transactions = NominalTransaction.objects.filter(nominal__name__in=nominal_names,
                                                    period__gte=period_from,
                                                    period__lte=period_to)

    context = {"transactions": transactions,
               "period_from": period_from,
               "period_to": period_to}
    return render(request, "nominal_transactions.html", context)
