
__all__ = ["FileHandle"]


_magic_dict = {
    b"\x1f\x8b\x08": "gz",
    b"\x42\x5a\x68": "bz2",
    b"\x50\x4b\x03\x04": "zip"
    }


_max_magic_len = max(len(x) for x in _magic_dict)


def _should_compress(filename, filesize):
    """Return whether or not the passed file is worth compressing.
       It is not worth compressing very small files (<128 bytes) or
       already-compressed files

       Args:
            filename (str): Filename
            filesize (int): Size of file in bytes
       Returns:
            bool: True if file should be compressed, else
            False
    """
    if filesize < 128:
        return False

    with open(filename, "rb") as FILE:
        file_start = FILE.read(_max_magic_len)

    for magic in _magic_dict.keys():
        if file_start.startswith(magic):
            return False

    return True


def _bz2compress(inputfile, outputfile=None):
    """Compress 'inputfile', writing the output to 'outputfile'
       If 'outputfile' is None, then this will create a new filename
       in the current directory for the file. This returns
       the filename for the compressed file

       Args:
            inputfile (str): File to compress
            outputfile (str, default=None): Name for compressed
            file
       Returns:
            str: Filename of compressed file

    """
    from Acquire.Client import compress as _compress
    return _compress(inputfile=inputfile, outputfile=outputfile,
                     compression_type="bz2")


class FileHandle:
    """This class holds all of the information about a file that is
       held in a Drive, including its size
       and checksum, and information about previous versions. It
       provides a handle that you can use to download or delete
       the file, to upload new versions, or to move the data between
       hot and cold storage or pay for extended storage

       Args:
            filename (str, default=None): File to create handle for
            remote_filename (str, default=None): Remote path for file
            aclrules (str, default=None): ACL rules for handle
            drive_uid (str, default=None): UID for drive
            compress (bool, default=True): Should files be compressed
            local_cutoff (int, default=None): Size of file to be held
            locally by the handle (bytes)

    """
    def __init__(self, filename=None, remote_filename=None,
                 aclrules=None, drive_uid=None,
                 compress=True, local_cutoff=None):
        """Construct a handle for the local file 'filename'. This will
           create the initial version of the file that can be uploaded
           to the storage service. If the file is less than
           'local_cutoff' bytes then the file will be held directly
           in this handle. By default local_cutoff is 1 MB
        """
        self._local_filename = None
        self._local_filedata = None
        self._compression = None
        self._compressed_filename = None
        self._drive_uid = drive_uid
        self._aclrules = None

        if filename is not None:
            if local_cutoff is None:
                local_cutoff = 1048576
            else:
                local_cutoff = int(local_cutoff)

            if aclrules is None:
                # will be automatically set to 'inherit' on the service
                self._aclrules = None
            else:
                from Acquire.Identity import ACLRules as _ACLRules
                self._aclrules = _ACLRules.create(rule=aclrules)

            from Acquire.Access import get_filesize_and_checksum \
                as _get_filesize_and_checksum
            import os as _os

            (filesize, cksum) = _get_filesize_and_checksum(filename=filename)

            if compress and _should_compress(filename=filename,
                                             filesize=filesize):
                import bz2 as _bz2
                if filesize < local_cutoff:
                    # this is not big, so better to compress in memory
                    from Acquire.Access import get_size_and_checksum \
                        as _get_size_and_checksum
                    data = open(filename, "rb").read()
                    data = _bz2.compress(data)
                    (filesize, cksum) = _get_size_and_checksum(data=data)
                    self._local_filedata = data
                    self._compression = "bz2"
                else:
                    # this is a bigger file, so compress on disk
                    try:
                        self._compressed_filename = _bz2compress(
                                                        inputfile=filename)
                    except:
                        pass

                    if self._compressed_filename is not None:
                        self._compression = "bz2"
                        (filesize, cksum) = _get_filesize_and_checksum(
                                            filename=self._compressed_filename)
            elif filesize < local_cutoff:
                # this is small enough to hold in memory
                self._local_filedata = open(filename, "rb").read()

            if self._compressed_filename is None:
                self._local_filename = filename

            self._filesize = filesize
            self._checksum = cksum

            if remote_filename is None:
                self._filename = _os.path.split(filename)[1]
            else:
                from Acquire.ObjectStore import string_to_filepath \
                    as _string_to_filepath
                self._filename = _string_to_filepath(remote_filename)
        else:
            self._filename = None

    def __del__(self):
        """Ensure we delete the temporary file before being destroyed"""
        if self._compressed_filename is not None:
            import os as _os
            _os.unlink(self._compressed_filename)
            self._compressed_filename = None

    def __str__(self):
        """Return a string representation of the file"""
        if self.is_null():
            return "FileHandle::null"

        return "FileHandle(filename='%s')" % self.filename()

    def is_null(self):
        """Return whether or not this this null

        Returns:
            bool: True if handle null, else False

        """
        return self._filename is None

    def is_compressed(self):
        """Return whether or not the file is compressed on transport

           Returns:
                bool: True if compressed, else False
        """
        return self._compression is not None

    def compression_type(self):
        """Return a string describing the compression scheme used by the
           filehandle when transporting the file, or None if the data
           is not compressed

           Returns:
                str: Compression type
        """
        return self._compression

    def is_localdata(self):
        """Return whether or not this file is so small that the data
           is held in memory

           Returns:
                bool: True if file held in memory, else False
        """
        return self._local_filedata is not None

    def local_filedata(self, decompress=False):
        """Return the filedata for this file, assuming it is sufficiently
           small to be read in this way. Returns 'None' if not...

           If 'decompress' is true, then decompress the data
           (if it is compressed) before returning

           Args:
                decompress (bool, default=False): If True decompress
                data locally
           Returns:
                None or str: If decompress=True, return None, else return
                filedata
        """
        if decompress and self.is_compressed():
            if self._local_filedata is not None:
                import bz2 as _bz2
                return _bz2.decompress(self._local_filedata)
            else:
                return None
        else:
            return self._local_filedata

    def local_filename(self):
        """Return the local filename for this file

           Returns:
                str: Local filename for file
        """
        if self.is_localdata():
            return None
        elif self.is_compressed():
            return self._compressed_filename
        else:
            return self._local_filename

    def drive_uid(self):
        """Return the UID of the drive on which this file is located

           Returns:
                str: UID for drive
        """
        return self._drive_uid

    def aclrules(self):
        """Return the ACL rules for this file

           Returns:
                str: ACL rules for file
        """
        return self._aclrules

    def filename(self):
        """Return the remote (object store) filename for this file

           Returns:
                str: Filename
        """
        return self._filename

    def filesize(self):
        """Return the size (in bytes) of this file

           Returns:
                int: Size of file in bytes
        """
        if self.is_null():
            return 0
        else:
            return self._filesize

    def checksum(self):
        """Return the checksum of the contents of this file

           Returns:
                str: MD5 checksum for file
        """
        if self.is_null():
            return None
        else:
            return self._checksum

    def fingerprint(self):
        """Return a fingerprint for this file

           Returns:
                str: Fingerprint for file consisting of
                filename, file size and MD5 checksum
        """
        return "%s:%s:%s" % (self.filename(),
                             self.filesize(), self.checksum())

    def to_data(self):
        """Return a json-serialisable dictionary for this object. Note
           that this does not contain any information about the local
           file itself - just the name it should be called on the
           object store and the size, checksum and acl. If the file
           (or compressed file) is sufficiently small then this
           will also contain the packed version of that file data

           Returns:
                dict: JSON serialisable dictionary of object
        """
        data = {}

        if not self.is_null():
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            data["filename"] = self.filename()
            data["filesize"] = self.filesize()
            data["checksum"] = self.checksum()

            if self._aclrules is not None:
                data["aclrules"] = self._aclrules.to_data()

            data["drive_uid"] = self.drive_uid()

            if self._local_filedata is not None:
                from Acquire.ObjectStore import bytes_to_string \
                    as _bytes_to_string
                data["filedata"] = _bytes_to_string(self._local_filedata)

            if self._compression is not None:
                data["compression"] = self._compression

        return data

    @staticmethod
    def from_data(data):
        """Return an object created from the passed json-deserialised
           dictionary. Note that this does not contain any information
           about the local file itself - just the name it should be
           called on the object store and the size, checksum and acl.
           If the file (or compressed file) is sufficiently small then this
           will also contain the packed version of that file data

           Args:
                data (dict): JSON-deserialised dictionary
           Returns:
                FileHandle: FileHandle object created from dictionary
        """
        f = FileHandle()

        if data is not None and len(data) > 0:
            from Acquire.Storage import ACLRule as _ACLRule
            f._filename = data["filename"]
            f._filesize = int(data["filesize"])
            f._checksum = data["checksum"]
            f._drive_uid = data["drive_uid"]

            if "compression" in data:
                f._compression = data["compression"]

            if "aclrules" in data:
                from Acquire.Storage import ACLRules as _ACLRules
                f._aclrules = _ACLRules.from_data(data["aclrules"])

            if "filedata" in data:
                from Acquire.ObjectStore import string_to_bytes \
                    as _string_to_bytes

                f._local_filedata = _string_to_bytes(data["filedata"])

        return f
