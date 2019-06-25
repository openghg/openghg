
__all__ = ["ChunkUploader"]


class ChunkUploader:
    """This class is used to control the chunked uploading
       of a file. This allows a file to be uploaded
       chunk by chunk (bit by bit). This is useful, e.g.
       to upload a file as it is being written
    """
    def __init__(self, drive_uid=None, file_uid=None):
        """Create a new ChunkUploader that uploads the specified
           file to the specified drive
        """
        self._drive_uid = None
        self._file_uid = None
        self._chunk_idx = None
        self._service = None

        if drive_uid is not None:
            self._drive_uid = str(drive_uid)
            self._file_uid = str(file_uid)
            from Acquire.Crypto import PrivateKey as _PrivateKey
            self._secret = _PrivateKey.random_passphrase()

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
        return self._drive_uid is None or self._file_uid is None

    def secret(self):
        """Return the secret used to authenticate the upload"""
        return self._secret

    def service(self):
        """Return the service that created this uploader"""
        return self._service

    def upload(self, chunk):
        """Upload the next chunk of the file"""
        if self.is_null():
            raise PermissionError("Cannot upload a chunk to a null uploader!")

        service = self.service()

        if service is None:
            raise PermissionError("Cannot upload a chunk to a null service!")

        # first, compress the chunk
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
        from Acquire.Crypto import Hash as _Hash
        import bz2 as _bz2

        if isinstance(chunk, str):
            chunk = chunk.encode("utf-8")

        chunk = _bz2.compress(chunk)
        md5 = _Hash.md5(chunk)
        chunk = _bytes_to_string(chunk)

        if self._chunk_idx is None:
            self._chunk_idx = 0
        else:
            self._chunk_idx = self._chunk_idx + 1

        secret = _Hash.multi_md5(self._secret,
                                 "%s%s%d" % (self._drive_uid,
                                             self._file_uid,
                                             self._chunk_idx))

        args = {}
        args["drive_uid"] = self._drive_uid
        args["file_uid"] = self._file_uid
        args["chunk_index"] = self._chunk_idx
        args["secret"] = secret
        args["data"] = chunk
        args["checksum"] = md5

        service.call_function(function="upload_chunk", args=args)

    def is_open(self):
        """Return whether or not the file is open (has been written to)"""
        return self._chunk_idx is not None

    def close(self):
        """Close the uploader - this will finalise the file"""
        if self.is_open():
            args = {"drive_uid": self._drive_uid,
                    "file_uid": self._file_uid,
                    "secret": self._secret}

            self.service().call_function(function="close_uploader",
                                         args=args)

            self._chunk_idx = None
            self._secret = None
            self._drive_uid = None
            self._file_uid = None

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
        """Return a ChunkUploader from a json-deserialised dictionary.
           If this was encrypted then you need to supply a private
           key to decrypt the sensitive data
        """
        if data is None or len(data) == 0:
            return ChunkUploader()

        c = ChunkUploader()

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

        c._drive_uid = data["drive_uid"]
        c._file_uid = data["file_uid"]
        c._secret = data["secret"]
        c._service = service

        return c
