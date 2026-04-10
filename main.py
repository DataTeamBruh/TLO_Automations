from overdue_accounts import main as overdue_main
from financial_accounts import main as financial_main
from negative_margins import main as margins_main
from Retainer_Deposit import run_retainer_job

def hello_http(request):
    job = request.args.get("job") if request.args else None

    if job == "overdue":
        overdue_main()
        return ("Overdue accounts job completed", 200)

    elif job == "financial":
        financial_main()
        return ("Financial accounts job completed", 200)

    elif job == "margins":
        margins_main()
        return ("Negative margins job completed", 200)

    elif job == "retainer":
        run_retainer_job()
        return ("Retainer job completed", 200)

    else:
        return (
            "No job specified. Use ?job=overdue | financial | margins | retainer",
            400,
        )