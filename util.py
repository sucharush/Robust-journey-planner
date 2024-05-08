def time_to_minutes(time_str):
    """Converts a time string to minutes in a day."""
    hours, minutes, seconds = map(int, time_str.split(':'))
    return hours * 60 + minutes + seconds / 60

def minutes_to_hours(minutes):
    """Converts minutes in a day to a time string."""
    hours = minutes // 60
    remaining_minutes = minutes % 60
    return str(int(hours)) + ":" + str(int(remaining_minutes))