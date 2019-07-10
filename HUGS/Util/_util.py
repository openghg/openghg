""" Utility functions that are used by multiple modules

"""

__all__ = ["url_join", "get_daterange_str", "get_datetime_epoch", 
            "get_datetime_now", "get_datetime", "unanimous"]

def url_join(*args):
    """ Joins given arguments into an filepath style key. Trailing but not leading slashes are
        stripped for each argument.

        Args:
            *args (str): Strings to concatenate into a key to use
            in the object store
        Returns:
            str: A url style key string with arguments separated by forward slashes
    """
    return "/".join(map(lambda x: str(x).rstrip('/'), args))

def get_daterange_str(start, end):
    """ Creates a string from the start and end datetime
        objects. Used for production of the key
        to store segmented data in the object store.

        Args:
            start (datetime): Start datetime
            end (datetime): End datetime
        Returns:
            str: Daterange formatted as start_end
            YYYYMMDD_YYYYMMDD
            Example: 20190101_20190201
    """

    start_fmt = start.strftime("%Y%m%d")
    end_fmt = end.strftime("%Y%m%d")

    return start_fmt + "_" + end_fmt

def get_datetime_epoch():
    """ Returns the UNIX epoch time
        1st of January 1970

        Returns:
            datetime: Datetime object at epoch
    """
    import datetime as _datetime

    return _datetime.datetime(1970, 1, 1, 0, 0, tzinfo=_datetime.timezone.utc)

def get_datetime_now():
    """ Returns the UNIX epoch time
        1st of January 1970

        Returns:
            datetime: Datetime object at epoch
    """
    import datetime as _datetime

    return _datetime.datetime.utcnow().replace(tzinfo=_datetime.timezone.utc)

def get_datetime(year, month, day, hour=None, minute=None, second=None):
    """ Returns a timezone aware datetime object

        Args:
            year (int): Year
            month (int): Month of year
            day (int): Day of month
            hour (int, default=None): Hour of day
            minute (int, default=None): Minute of hour
            second (int, default=None): Second of minute
        Returns:
            datetime: Timezone aware datetime object
    """
    from datetime import datetime as _datetime
    from Acquire.ObjectStore import datetime_to_datetime as _datetime_to_datetime

    date = _datetime(year=year, month=month, day=day)#, hour=hour, minute=minute, second=second)

    return _datetime_to_datetime(date)

def unanimous(seq):
            """ Checks that all values in an iterable object
                are the same

                Args:
                    seq: Iterable object
                Returns
                    bool: True if all values are the same

            """
            it = iter(seq.values())
            try:
                first = next(it)
            except StopIteration:
                return True
            else:
                return all(i == first for i in it)
