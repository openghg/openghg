
__all__ = ["get_filesize_and_checksum", "get_size_and_checksum"]


def get_size_and_checksum(data):
    """Calculates the size and md5 of the passed data

       Args:
            data (byte): data to calculate checksum for
        Returns:
            tuple (int,str): size of data and its md5 hash
    """
    from hashlib import md5 as _md5
    md5 = _md5()
    md5.update(data)

    return (len(data), str(md5.hexdigest()))


def get_filesize_and_checksum(filename):
    """Opens the file with the passed filename and calculates
        its size and md5 hash

       Args:
            filename (str): filename to calculate size and checksum for
        Returns:
            tuple (int,str): size of data and its md5 hash

    """

    from hashlib import md5 as _md5
    md5 = _md5()
    size = 0

    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
            size += len(chunk)

    return (size, str(md5.hexdigest()))
