from overdue_accounts import main as overdue_main

def hello_http(request):
    overdue_main()
    return ("Overdue accounts job completed", 200)