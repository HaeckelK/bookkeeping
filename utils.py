def convert_date_string_to_period(timestamp) -> int:
    print("entered here", timestamp)
    try:
        month = int(timestamp.month)
    except AttributeError:
        print("went wrong here")
        return -1
    else:
        return month
