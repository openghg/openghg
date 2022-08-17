import base64
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


def string_to_bytes(s: str) -> bytes:
    """Return the passed base64 utf-8 encoded binary data
    back converted from a string back to bytes. Note that
    this can only convert strings that were encoded using
    bytes_to_string - you cannot use this to convert
    arbitrary strings to bytes

    Args:
        s: base64 byte object to decode
    Returns:
        bytes: bytes
    """
    if s is None:
        return None
    else:
        return base64.b64decode(s.encode("utf-8"))


def bytes_to_string(b: bytes) -> str:
    """Return the passed binary bytes safely encoded to
    a base64 utf-8 string

    Args:
        b: binary bytes to encode
    Returns:
        str: UTF-8 encoded string
    """
    if b is None:
        return None
    else:
        return base64.b64encode(b).decode("utf-8")
