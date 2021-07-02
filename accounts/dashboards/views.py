from django.shortcuts import render, HttpResponse

from .models import PeriodBalance


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
