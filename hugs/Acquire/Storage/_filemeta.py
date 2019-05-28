
__all__ = ["FileMeta"]


class FileMeta:
    """This is a lightweight class that holds the metadata about
       a particular version of a file
    """
    def __init__(self, filename=None, uid=None, filesize=None,
                 checksum=None, uploaded_by=None, uploaded_when=None,
                 compression=None, aclrules=None):
        """Construct, specifying the filename, and then optionally
           other useful data
        """
        self._filename = filename
        self._uid = uid
        self._filesize = filesize
        self._checksum = checksum
        self._user_guid = uploaded_by
        self._datetime = uploaded_when
        self._compression = compression
        self._acl = None
        self._aclrules = None

        if aclrules is not None:
            from Acquire.Storage import ACLRules as _ACLRules
            if not isinstance(aclrules, _ACLRules):
                raise TypeError("The aclrules must be type ACLRules")

            self._aclrules = aclrules

    def __str__(self):
        """Return a string representation"""
        if self.is_null():
            return "FileMeta::null"
        else:
            return "FileMeta(%s)" % self._filename

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        """Comparison equals"""
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def _set_denied(self):
        """Call this function to remove all information that should
           not be visible to someone who has denied access to the file
        """
        self._uid = None
        self._filesize = None
        self._checksum = None
        self._user_guid = None
        self._datetime = None
        self._compression = None
        self._aclrules = None
        from Acquire.Storage import ACLRule as _ACLRule
        self._acl = _ACLRule.denied()

    def is_null(self):
        """Return whether or not this is null"""
        return self._filename is None

    def has_metadata(self):
        """Return whether or not this file includes all of the
           metadata. If not, then only the filename is available
        """
        return self._uid is not None

    def _set_drive(self, drive):
        """Internal function called by "Drive" to store the Drive
           associated with a FileMeta
        """
        from Acquire.Client import Drive as _Drive
        if not isinstance(drive, _Drive):
            raise TypeError("You can only set a Drive object")

        self._drive = drive

    def filename(self):
        """Return the name of the file"""
        return self._filename

    def uid(self):
        """Return the UID of the file in the system"""
        if self.is_null():
            return None
        else:
            return self._uid

    def filesize(self):
        """If known, return the size of the file"""
        if self.is_null():
            return None
        else:
            return self._filesize

    def checksum(self):
        """If known, return a checksum of the file"""
        if self.is_null():
            return None
        else:
            return self._checksum

    def is_compressed(self):
        """If known, return whether or not this file is stored and
           transmitted in a compressed state
        """
        if self.is_null():
            return False
        else:
            return self._compression is not None

    def compression_type(self):
        """Return the compression type for this file, if it is
           stored and transmitted in a compressed state
        """
        if self.is_null():
            return None
        else:
            return self._compression

    def uploaded_by(self):
        """If known, return the GUID of the user who uploaded
           this version of the file
        """
        if self.is_null():
            return None
        else:
            return self._user_guid

    def uploaded_when(self):
        """If known, return the datetime when this version of
           the file was uploaded
        """
        if self.is_null():
            return None
        else:
            return self._datetime

    def aclrules(self):
        """If known, return the ACL rules that were used to generate the ACL
           for this file. Note that you can only see the ACL rules if
           you are an owner of the file
        """
        try:
            return self._aclrules
        except:
            return None

    def resolve_acl(self, identifiers=None, upstream=None,
                    must_resolve=None, unresolved=False):
        """Resolve the ACL for this file based on the passed arguments
           (same as for ACLRules.resolve()). This returns the resolved
           ACL, which is set as self.acl()
        """
        aclrules = self.aclrules()
        if aclrules is None:
            raise PermissionError(
                "You do not have permission to resolve the ACLs for this file")

        self._acl = aclrules.resolve(must_resolve=must_resolve,
                                     identifiers=identifiers,
                                     upstream=upstream,
                                     unresolved=unresolved)

        if not self._acl.is_owner():
            # only owners can see the ACLs
            self._aclrules = None

        if self._acl.denied_all():
            self._set_denied()

        return self._acl

    def acl(self):
        """If known, return the ACL for this version of the file for the
           user that requested this FileMeta (e.g. the user who listed
           the drive containing this file)
        """
        try:
            return self._acl
        except:
            return None

    def assert_correct_data(self, filedata=None, filename=None):
        """Assert that the passed data is correct (right size and
           checksum)
        """
        if filedata is not None:
            from Acquire.Access import get_size_and_checksum \
                as _get_size_and_checksum
            (filesize, checksum) = _get_size_and_checksum(filedata)
        else:
            from Acquire.Access import get_filesize_and_checksum \
                as _get_filesize_and_checksum
            (filesize, checksum) = _get_filesize_and_checksum(filename)

        if (filesize != self._filesize) or (checksum != self._checksum):
            from Acquire.Storage import FileValidationError
            raise FileValidationError(
                "Possible data corruption. Mismatch in file size or "
                "checksum for file '%s'. "
                "%s versus %s, and %s versus %s" %
                (self._filename, filesize, self._filesize,
                 checksum, self._checksum))

    def to_data(self):
        """Return a json-serialisable dictionary of this object"""
        data = {}

        if self.is_null():
            return data

        data["filename"] = str(self._filename)

        if self._uid is not None:
            data["uid"] = str(self._uid)

        if self._filesize is not None:
            data["filesize"] = self._filesize

        if self._checksum is not None:
            data["checksum"] = self._checksum

        if self._user_guid is not None:
            data["user_guid"] = self._user_guid

        if self._compression is not None:
            data["compression"] = self._compression

        try:
            acl = self._acl
        except:
            acl = None

        if acl is not None:
            data["acl"] = acl.to_data()

        try:
            aclrules = self._aclrules
        except:
            aclrules = None

        if aclrules is not None:
            data["aclrules"] = aclrules.to_data()

        if self._datetime is not None:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            data["datetime"] = _datetime_to_string(self._datetime)

        return data

    @staticmethod
    def from_data(data):
        """Return a new FileMeta constructed from the passed json-deserialised
           dictionary
        """
        f = FileMeta()

        if data is not None and len(data) > 0:
            f._filename = data["filename"]

            if "uid" in data:
                f._uid = data["uid"]

            if "filesize" in data:
                f._filesize = data["filesize"]

            if "checksum" in data:
                f._checksum = data["checksum"]

            if "user_guid" in data:
                f._user_guid = data["user_guid"]

            if "datetime" in data:
                from Acquire.ObjectStore import string_to_datetime \
                    as _string_to_datetime
                f._datetime = _string_to_datetime(data["datetime"])

            if "compression" in data:
                f._compression = data["compression"]

            if "acl" in data:
                from Acquire.Client import ACLRule as _ACLRule
                f._acl = _ACLRule.from_data(data["acl"])

            if "aclrules" in data:
                from Acquire.Storage import ACLRules as _ACLRules
                f._aclrules = _ACLRules.from_data(data["aclrules"])

        return f
