def convert_date_string_to_period(timestamp) -> int:
    try:
        month = int(timestamp.month)
    except AttributeError:
        return -1
    else:
        return month
