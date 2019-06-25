

class Location:
    """This class holds a globally-resolvable location for a
       file or Drive.
    """
    def __init__(self, drive_guid=None, filename=None, version=None):
        """Construct an Location. This uses the GUID of the
           drive to identify the drive, and then (optionally) the
           name of the file in the drive, and then the specific
           version.

           The drive GUID has the format {service_uid}/{drive_uid}

           If the filename is not supplied, then this locates
           the drive itself. If a version is not supplied, then this
           locates the latest version of the file
        """
        self._drive_guid = drive_guid
        self._encoded_filename = None
        self._filename = None
        self._version = None

        if self._drive_guid is not None:
            self._filename = filename
            self._version = version

            if self._filename is not None:
                from Acquire.ObjectStore import string_to_encoded \
                    as _string_to_encoded
                self._encoded_filename = _string_to_encoded(self._filename)
            else:
                self._encoded_filename = None

    def is_null(self):
        return self._drive_guid is None

    def __str__(self):
        if self.is_null():
            return "Location::null"
        elif self.is_drive():
            return "acquire_drive://%s" % self._drive_guid
        elif self.specifies_version():
            return "acquire_file://%s/%s/%s" % (self._drive_guid,
                                                self._encoded_filename,
                                                self._version)
        else:
            return "acquire_file://%s/%s" % (self._drive_guid,
                                             self._encoded_filename)

    def fingerprint(self):
        """Return a fingerprint that can be used to show that the
           user has authorised something to do with this location
        """
        return str(self)

    def to_string(self):
        """Return a safe string that completely describes this
           location
        """
        return str(self)

    @staticmethod
    def from_string(s):
        """Return an Location constructed from the passed string"""
        if s.startswith("acquire_file://"):
            parts = s.split("/")
            from Acquire.ObjectStore import encoded_to_string \
                as _encoded_to_string

            try:
                drive_guid = parts[-2]
                filename = _encoded_to_string(parts[-1])
                version = None
            except:
                drive_guid = parts[-4]
                filename = _encoded_to_string(parts[-3])
                version = "/".join([parts[-2], parts[-1]])

            return Location(drive_guid=drive_guid, filename=filename,
                            version=version)

        elif s.startswith("acquire_drive://"):
            parts = s.split("/")
            return Location(drive_guid=parts[-1])
        else:
            return Location()

    def is_file(self):
        """Return whether or not this is a location for a file"""
        if self.is_null():
            return False
        else:
            return self._encoded_filename is not None

    def is_drive(self):
        """Return whether or not this is a location for a drive"""
        if self.is_null():
            return False
        else:
            return self._encoded_filename is None

    def specifies_version(self):
        """Return whether or not this specifies the version of the file"""
        if self.is_null():
            return False
        else:
            return self._version is not None

    def drive_guid(self):
        """Return the GUID of the drive"""
        if self.is_null():
            return None
        else:
            return self._drive_guid

    def drive_uid(self):
        """Return the UID of the drive"""
        if self.is_null():
            return None
        else:
            return self._drive_guid.split("@")[0]

    def filename(self):
        """Return the name of the file (if this is a file)"""
        return self._filename

    def encoded_filename(self):
        """Return the URL-safe encoded filename (if this a file)"""
        return self._encoded_filename

    def version(self):
        """Return the version of the file (if this is a file and
           the version has been specified)
        """
        return self._version

    def service_uid(self):
        """Return the UID of the service that holds the File/Drive
           behind this location
        """
        if self.is_null():
            return None

        return self._drive_guid.split("@")[-1]

    def service(self):
        """Return the service that holds the File/Drive behind this
           location
        """
        if self.is_null():
            return None

        from Acquire.Service import get_trusted_service \
            as _get_trusted_service

        return _get_trusted_service(service_uid=self.service_uid())

    def service_url(self):
        """Return the URL of the service that holds the File/Drive
           behind this location
        """
        if self.is_null():
            return None

        return self.service().canonical_url()

    @staticmethod
    def from_data(data):
        """Return an Location constructed from the json-deserialised
           dictionary
        """
        if data is None or len(data) == 0:
            return Location()

        iden = Location()
        iden._drive_guid = data["drive_guid"]

        try:
            iden._encoded_filename = data["encoded_filename"]
        except:
            return iden

        from Acquire.ObjectStore import encoded_to_string \
            as _encoded_to_string
        iden._filename = _encoded_to_string(iden._encoded_filename)

        try:
            iden._version = data["version"]
        except:
            pass

        return iden

    def to_data(self):
        """Return a json-serialisable dictionary of this object"""
        if self.is_null():
            return None

        data = {}
        data["drive_guid"] = self.drive_guid()

        if self.is_file():
            data["encoded_filename"] = self.encoded_filename()

        if self.specifies_version():
            data["version"] = self.version()

        return data
