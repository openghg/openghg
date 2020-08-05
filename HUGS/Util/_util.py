""" Utility functions that are used by multiple modules

"""

__all__ = [
    "url_join",
    "get_daterange_str",
    "get_datetime_epoch",
    "get_datetime_now",
    "get_datetime",
    "unanimous",
    "load_object",
    "hash_file",
    "timestamp_tzaware",
    "get_datapath",
    "date_overlap",
    "read_header",
    "load_hugs_json",
    "valid_site",
    "daterange_from_str",
    "daterange_to_str",
    "daterange_from_datetimes"
]


def url_join(*args):
    """ Joins given arguments into an filepath style key. Trailing but not leading slashes are
        stripped for each argument.

        Args:
            *args (str): Strings to concatenate into a key to use
            in the object store
        Returns:
            str: A url style key string with arguments separated by forward slashes
    """
    return "/".join(map(lambda x: str(x).rstrip("/"), args))


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
    from datetime import datetime
    from Acquire.ObjectStore import datetime_to_datetime

    date = datetime(
        year=year, month=month, day=day
    )  # , hour=hour, minute=minute, second=second)

    return datetime_to_datetime(date)


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


def load_object(class_name):
    """ Load an object of type class_name

        Args:
            class_name (str): Name of class to load
        Returns:
            class_name: class_name object
    """
    module_path = "HUGS.Modules"
    class_name = str(class_name).upper()

    # Here we try upper and lowercase for the module
    try:
        # Although __import__ is not usually recommended, here we want to use the
        # fromlist argument that import_module doesn't support
        module_object = __import__(name=module_path, fromlist=class_name)
        target_class = getattr(module_object, class_name)
    except AttributeError:
        class_name = class_name.lower().capitalize()
        module_object = __import__(name=module_path, fromlist=class_name)
        target_class = getattr(module_object, class_name)
    except ModuleNotFoundError as err:
        raise ModuleNotFoundError(f"{class_name} is not a valid module {err}")

    return target_class.load()


def hash_file(filepath):
    """ Opens the file at filepath and calculates its SHA1 hash

        Taken from https://stackoverflow.com/a/22058673

        Args:
            filepath (pathlib.Path): Path to file
        Returns:
            str: SHA1 hash
    """
    import hashlib

    # Lets read stuff in 64kb chunks
    BUF_SIZE = 65536
    sha1 = hashlib.sha1()

    with open(filepath, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)

    return sha1.hexdigest()


def timestamp_tzaware(timestamp):
    """ Returns the pandas Timestamp passed as a timezone (UTC) aware
        Timestamp.

        Args:
            timestamp (pandas.Timestamp): Timezone naive Timestamp
        Returns:
            pandas.Timestamp: Timezone aware
    """
    if timestamp.tzinfo is None:
        return timestamp.tz_localize(tz="UTC")
    else:
        return timestamp.tz_convert(tz="UTC")


def get_datapath(filename, directory=None):
    """ Returns the correct path to JSON files used for assigning attributes

        Args:
            filename (str): Name of JSON file
        Returns:
            pathlib.Path: Path of file
    """
    from pathlib import Path

    filename = str(filename)

    if directory is None:
        return Path(__file__).resolve().parent.parent.joinpath(f"Data/{filename}")
    else:
        return (
            Path(__file__)
            .resolve()
            .parent.parent.joinpath(f"Data/{directory}/{filename}")
        )


def load_hugs_json(filename):
    """ Returns a dictionary created from the HUGS JSON at filename

        Args:
            filename (str): Name of JSON file
        Returns:
            dict: Dictionary created from JSON
    """
    from json import load

    path = get_datapath(filename)

    with open(path, "r") as f:
        data = load(f)

    return data


def date_overlap(daterange_a, daterange_b):
    """ Check if daterange_a is within daterange_b

        Args:
            daterange_a (str): Timezone aware daterange string. Example:
            2014-01-30-10:52:30+00:00_2014-01-30-13:22:30+00:00
            daterange_b (str): As daterange_a
        Returns:
            bool: True if daterange included
    """
    from pandas import Timestamp

    daterange_a = daterange_a.split("_")
    daterange_b = daterange_b.split("_")

    start_a = Timestamp(ts_input=daterange_a[0], tz="UTC")
    end_a = Timestamp(ts_input=daterange_a[1], tz="UTC")

    start_b = Timestamp(ts_input=daterange_b[0], tz="UTC")
    end_b = Timestamp(ts_input=daterange_b[1], tz="UTC")

    if start_b >= start_a and end_a <= end_b:
        return True
    else:
        return False


def daterange_from_datetimes(start, end):
    """ Convert the passed datetimes into a daterange string
        for use in searches and Datasource interactions

        Args:
            start_datetime (datetime)
            end_datetime (datetime)
        Returns:
            str: Daterange string
    """
    from pandas import date_range

    daterange = date_range(start=start, end=end, freq="min")

    return daterange_to_str(daterange)


def daterange_from_str(daterange_str):
    """ Get a Pandas DatetimeIndex from a string. The created 
        DatetimeIndex has minute frequency.

        Args:
            daterange_str (str): Daterange string
            of the form 2019-01-01T00:00:00_2019-12-31T00:00:00
        Returns:
            pandas.DatetimeIndex: DatetimeIndex with minute frequency
    """
    from pandas import date_range
    from pandas import Timestamp

    split = daterange_str.split("_")

    start = Timestamp(split[0], tz="UTC")
    end = Timestamp(split[1], tz="UTC")

    return date_range(start=start, end=end, freq="min")


def daterange_to_str(daterange):
    """ Takes a pandas DatetimeIndex created by pandas date_range converts it to a
        string of the form 2019-01-01-00:00:00_2019-03-16-00:00:00

        Args:
            daterange (pandas.DatetimeIndex)
        Returns:
            str: Daterange in string format
    """
    start = str(daterange[0]).replace(" ", "-")
    end = str(daterange[-1]).replace(" ", "-")

    return "_".join([start, end])


def read_header(filepath, comment_char="#"):
    """ Reads the header lines denoted by the comment_char

        Args:
            filepath (str or Path): Path to file
            comment_char (str, default="#"): Character that denotes a comment line
            at the start of a file
    """
    comment_char = str(comment_char)

    header = []
    # Get the number of header lines
    with open(filepath, "r") as f:
        for line in f:
            if line.startswith(comment_char):
                header.append(line)
            else:
                break

    return header


def valid_site(site):
    """ Check if the passed site is a valid one

        Args:
            site (str): Three letter site code
        Returns:
            bool: True if site is valid
    """
    site_data = load_hugs_json("acrg_site_info.json")

    site = site.upper()

    return site in site_data
