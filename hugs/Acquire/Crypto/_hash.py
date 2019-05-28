
__all__ = ["Hash"]


class Hash:
    """This provides a static interface to a range of checksum
       (hashing) functions. By default we use MD5
    """
    @staticmethod
    def md5(data):
        """Return the MD5 checksum of the passed data"""
        if data is None:
            return None

        from hashlib import md5 as _md5

        if isinstance(data, str):
            data = data.encode("utf-8")

        md5 = _md5()
        md5.update(data)
        return md5.hexdigest()

    @staticmethod
    def multi_md5(data1, data2):
        """Return a combined MD5 checksum of data1 and data2 via

           MD5(MD5(data1) + MD5(data2))

           This heavily salts the MD5s of both bits of data
        """
        return Hash.md5(Hash.md5(data1)+Hash.md5(data2))
