import datetime


def datetime_to_datetime(d: datetime.datetime) -> datetime.datetime:
    """Return the passed datetime as a datetime that is clean
    and usable by Acquire. This will move the datetime to UTC,
    adding the timezone if this is missing

    Args:
         d (datetime): datetime to convert to UTC
    Returns:
         datetime: UTC datetime useable by Acquire
    """
    if not isinstance(d, datetime.datetime):
        raise TypeError(f"The passed object {str(d)} is not a valid datetime")

    if d.tzinfo is None:
        return d.replace(tzinfo=datetime.timezone.utc)
    else:
        return d.astimezone(datetime.timezone.utc)


def get_datetime_now():
    """Return the current time in the UTC timezone. This creates an
    object that will be properly stored using datetime_to_string
    and string_to_datetime

    Returns:
         datetime: Current datetime
    """
    return datetime.datetime.now(tz=datetime.timezone.utc)
