def time_to_minutes(time_str):
    """Converts a time string to minutes in a day."""
    hours, minutes, seconds = map(int, time_str.split(':'))
    return hours * 60 + minutes + seconds / 60

def minutes_to_hours(minutes):
    """Converts minutes in a day to a time string."""
    hours = minutes // 60
    remaining_minutes = minutes % 60
    return str(int(hours)) + ":" + str(int(remaining_minutes))


def process_stop_names(s):
    first_part = s.split(':')[0]  
    numbers_only = ''.join([ch for ch in first_part if ch.isdigit()])
    return numbers_only


def process_column_names(df):
    df.columns = [col.split('.')[1] for col in df.columns]
    return df


def time_to_minutes2(time_str):
    """Converts a time string to minutes in a day."""
    hours, minutes= map(int, time_str.split(':'))
    return hours * 60 + minutes


def minutes_to_hours(minutes):
    """Converts minutes in a day to a time string."""
    hours = minutes // 60
    remaining_minutes = minutes % 60
    if remaining_minutes < 10:
        return str(int(hours)) + ":0" + str(int(remaining_minutes))
    else:
        return str(int(hours)) + ":" + str(int(remaining_minutes))
