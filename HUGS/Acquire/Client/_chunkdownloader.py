
__all__ = ["ChunkDownloader"]


class ChunkDownloader:
    """This class is used to control the chunked downloading
       of a file. This allows a file to be downloaded
       chunk by chunk (bit by bit). This is useful, e.g.
       to download a file as it is being written
    """
    def __init__(self, drive_uid=None, file_uid=None):
        """Create a new ChunkDowloader that downloads the specified
           file from the specified drive
        """
        self._uid = None
        self._drive_uid = None
        self._file_uid = None
        self._service = None

        self._next_index = None
        self._last_filename = None
        self._downloaded_filename = None
        self._FILE = None

        if drive_uid is not None:
            self._drive_uid = str(drive_uid)
            self._file_uid = str(file_uid)
            from Acquire.Crypto import PrivateKey as _PrivateKey
            self._secret = _PrivateKey.random_passphrase()

            from Acquire.ObjectStore import create_uid as _create_uid
            self._uid = _create_uid(short_uid=True)

    def __exit__(self, exception_type, exception_value, traceback):
        """Ensure that we close the file"""
        if self.is_open():
            self.close()

    def __del__(self):
        """Make sure that we close the file before deleting this object"""
        if self.is_open():
            self.close()

    def is_null(self):
        """Return whether or not this is null"""
        return self._uid is None

    def uid(self):
        """Return the UID of this downloader"""
        return self._uid

    def secret(self):
        """Return the secret used to authenticate the download"""
        return self._secret

    def service(self):
        """Return the service that created this downloader"""
        return self._service

    def local_filename(self):
        """Return the name of the local file to which we are downloading
           data
        """
        return self._downloaded_filename

    def _start_download(self, filename=None, dir=None):
        """Start the download of the file to 'filename' in 'dir'"""
        if self.is_null():
            raise PermissionError(
                "Cannot download a chunk using a null uploader!")

        if filename is None:
            filename = self._last_filename

        if filename is None:
            raise PermissionError(
                "You must supply a filename to which to download the file!")

        if self._last_filename is None:
            self._last_filename = filename
            self._next_index = 0
            from Acquire.Client import create_new_file as \
                _create_new_file
            self._downloaded_filename = _create_new_file(filename=filename,
                                                         dir=dir)
            self._FILE = open(self._downloaded_filename, "ab")
        elif self._last_filename != filename:
            raise PermissionError(
                "You cannot change the filename during an active "
                "streaming download!")

        return self._downloaded_filename

    def download_next_chunk(self):
        """Download the next chunk. Returns 'True' if something was
           downloaded, else it returns 'False'
        """
        if not self.is_open():
            return False

        service = self.service()

        if service is None:
            raise PermissionError(
                "Cannot download a chunk from a null service!")

        from Acquire.Crypto import Hash as _Hash

        secret = _Hash.multi_md5(self._secret,
                                 "%s%s%d" % (self._drive_uid,
                                             self._file_uid,
                                             self._next_index))

        args = {}
        args["uid"] = self._uid
        args["drive_uid"] = self._drive_uid
        args["file_uid"] = self._file_uid
        args["chunk_index"] = self._next_index
        args["secret"] = secret

        response = service.call_function(function="download_chunk",
                                         args=args)

        if "meta" in response:
            import json as _json
            meta = _json.loads(response["meta"])
            checksum = meta["checksum"]

            from Acquire.ObjectStore import string_to_bytes \
                as _string_to_bytes

            chunk = _string_to_bytes(response["chunk"])

            md5 = _Hash.md5(chunk)

            if checksum != md5:
                from Acquire.Storage import FileValidationError
                raise FileValidationError(
                    "Problem downloading - checksums don't agree: %s vs %s" %
                    (checksum, md5))

            import bz2 as _bz2
            chunk = _bz2.decompress(chunk)
            self._FILE.write(chunk)
            self._FILE.flush()
            chunk = None

            self._next_index = self._next_index + 1

        if "num_chunks" in response:
            num_chunks = int(response["num_chunks"])

            if self._next_index >= num_chunks:
                # nothing more to download
                self.close()

        return True

    def download(self, filename=None, dir=None):
        """Download as much of the file as possible to 'filename'. You
           can call this repeatedly with the same filename (or with
           no filename set) to stream the file back as it is written
        """
        self._start_download(filename=filename, dir=dir)
        downloaded_filename = self._downloaded_filename

        got_chunk = self.download_next_chunk()

        while got_chunk:
            next_index = self._next_index
            got_chunk = self.download_next_chunk()

            if got_chunk:
                assert(next_index != self._next_index)

        return downloaded_filename

    def is_open(self):
        """Return whether or not the file is open (has been written to)"""
        return self._next_index is not None

    def close(self):
        """Close the downloader"""
        if self.is_open():
            args = {"uid": self._uid,
                    "drive_uid": self._drive_uid,
                    "file_uid": self._file_uid,
                    "secret": self._secret}

            self.service().call_function(function="close_downloader",
                                         args=args)

            self._FILE.close()
            self._last_filename = None
            self._downloaded_filename = None
            self._next_index = None
            self._secret = None
            self._drive_uid = None
            self._file_uid = None
            self._uid = None

    def to_data(self, pubkey=None):
        """Return a json-serialisable dictionary of the object. If
           'pubkey' is supplied, then sensitive data used by the
           uploader is encrypted
        """
        if self.is_null():
            return {}

        data = {}

        data["drive_uid"] = self._drive_uid
        data["file_uid"] = self._file_uid
        data["secret"] = self._secret
        data["uid"] = self._uid

        if pubkey is not None:
            from Acquire.Crypto import PublicKey as _PublicKey
            from Acquire.ObjectStore import bytes_to_string \
                as _bytes_to_string
            import json as _json

            if not isinstance(pubkey, _PublicKey):
                raise TypeError("pubkey must be type PublicKey")

            d = _bytes_to_string(pubkey.encrypt(_json.dumps(data)))

            data = {}
            data["is_encrypted"] = True
            data["data"] = d

        return data

    @staticmethod
    def from_data(data, privkey=None, service=None):
        """Return a ChunkDownloader from a json-deserialised dictionary.
           If this was encrypted then you need to supply a private
           key to decrypt the sensitive data
        """
        if data is None or len(data) == 0:
            return ChunkDownloader()

        c = ChunkDownloader()

        try:
            is_encrypted = data["is_encrypted"]
        except:
            is_encrypted = False

        if is_encrypted:
            if privkey is None:
                raise PermissionError("Cannot decode as encrypted!")

            from Acquire.Crypto import PrivateKey as _PrivateKey
            if not isinstance(privkey, _PrivateKey):
                raise TypeError("privkey must be type PrivateKey")

            from Acquire.ObjectStore import string_to_bytes \
                as _string_to_bytes
            import json as _json
            data = _json.loads(privkey.decrypt(_string_to_bytes(data["data"])))

        c._uid = data["uid"]
        c._drive_uid = data["drive_uid"]
        c._file_uid = data["file_uid"]
        c._secret = data["secret"]
        c._service = service

        return c
