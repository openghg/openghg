
__all__ = ["DriveMeta"]


class DriveMeta:
    """This is a lightweight class that holds the metadata about
       a Drive
    """
    def __init__(self, name=None, uid=None, container=None, aclrules=None):
        """Construct a drive with a specified name and (optionally) UID.
           'container' is the UID of the drive that contains this drive,
           at least for the user who has accessed this drive via a path.
           If 'container' is none, then the user is accessing this
           drive as a top-level drive
        """
        self._name = name
        self._uid = uid
        self._creds = None

        if isinstance(container, list):
            if len(container) == 1:
                container = [container]
            elif len(container) == 0:
                container = None

        self._container = container

        if aclrules is not None:
            from Acquire.Storage import ACLRules as _ACLRules
            if not isinstance(aclrules, _ACLRules):
                raise PermissionError(
                    "The ACL rules must be type ACLRules")

        self._aclrules = aclrules
        self._acl = None

    def __str__(self):
        """Return a string representation"""
        if self.is_null():
            return "DriveMeta::null"
        else:
            return "DriveMeta(%s)" % self._name

    def __repr__(self):
        return self.__str__()

    def is_null(self):
        """Return whether or not this is null"""
        return self._name is None

    def open(self, creds=None):
        """Open and return the Drive associated with this metadata"""
        from Acquire.Client import Drive as _Drive

        if creds is None:
            creds = self._creds

        return _Drive.open(metadata=self, creds=creds)

    def name(self):
        """Return the name of the drive"""
        return self._name

    def uid(self):
        """If known, return the UID of the drive"""
        return self._uid

    def acl(self):
        """If known, return the ACL rule for this drive"""
        return self._acl

    def aclrules(self):
        """If known, return the ACL rules for this drive"""
        return self._aclrules

    def location(self):
        """Return a global location for this Drive. This is unique
           for this Drive and can be used to locate
           it from any other service.
        """
        from Acquire.Client import Location as _Location
        return _Location(drive_guid=self.guid())

    def guid(self):
        """Return the globally unique identifier for this drive"""
        if self.is_null():
            return None
        elif self._creds is None:
            raise PermissionError(
                "Cannot generate the GUID as we don't know "
                "which storage service this Drive has come from!")
        else:
            return "%s@%s" % (self.uid(),
                              self._creds.storage_service().uid())

    def _set_credentials(self, creds):
        """Internal function used to set the credentials used
           to access this DriveMeta
        """
        from Acquire.Client import StorageCreds as _StorageCreds
        if not isinstance(creds, _StorageCreds):
            raise TypeError("The creds must be type StorageCreds")

        self._creds = creds

    def _set_denied(self):
        """Call this function to remove all information that should
           not be visible to someone who has denied access to the file
        """
        self._uid = None
        self._aclrules = None
        self._name = None
        self._container = None
        self._creds = None
        from Acquire.Storage import ACLRule as _ACLRule
        self._acl = _ACLRule.denied()

    def resolve_acl(self, identifiers=None, upstream=None,
                    must_resolve=None, unresolved=False,
                    open_aclrule=None):
        """Resolve the ACL for this file based on the passed arguments
           (same as for ACLRules.resolve()). This returns the resolved
           ACL, which is set as self.acl()
        """
        if self._aclrules is None:
            raise PermissionError(
                "You do not have permission to resolve the ACLs for "
                "this drive")

        self._acl = self._aclrules.resolve(identifiers=identifiers,
                                           upstream=upstream,
                                           must_resolve=must_resolve,
                                           unresolved=unresolved)

        if open_aclrule is not None:
            from Acquire.Client import ACLRule as _ACLRule
            if not isinstance(open_aclrule, _ACLRule):
                raise TypeError("The open_aclrule must be type ACLRule")

            open_aclrule = open_aclrule.resolve(must_resolve=True)
            self._acl = self._acl * open_aclrule

        if not self._acl.is_owner():
            # only owners can see the ACLs
            self._aclrules = None

        if self._acl.denied_all():
            self._set_denied()

        return self._acl

    def is_top_level(self):
        """Return whether or not this drive was accessed as a
           top-level drive
        """
        return self._container is None

    def container_uid(self):
        """Return the UID of the drive that contains this drive, at
           least via the access path followed by the user.
           This returns None if the user accessed this drive as
           a top-level drive
        """
        if self._container is None:
            return None
        else:
            return self._container[-1]

    def container_uids(self):
        """Return the UIDs of the full hierarchy of the drives that
           contain this drive, as this drive was accessed by the user.
           This returns an empty list if the user accessed this drive
           as a top-level drive
        """
        if self._container is None:
            return []
        else:
            return self._container

    def to_data(self):
        """Return a json-serialisable dictionary of this object"""
        data = {}

        if not self.is_null():
            data["name"] = str(self._name)

            if self._uid is not None:
                data["uid"] = str(self._uid)

            if self._container is not None:
                data["container"] = self._container

            if self._acl is not None:
                data["acl"] = self._acl.to_data()

        return data

    @staticmethod
    def from_data(data):
        """Return an object constructed from the passed json-deserialised
           dictionary
        """

        d = DriveMeta()

        if data is not None and len(data) > 0:
            d._name = data["name"]

            if "uid" in data:
                d._uid = data["uid"]

            if "container" in data:
                d._container = data["container"]

            if "acl" in data:
                from Acquire.Client import ACLRule as _ACLRule
                d._acl = _ACLRule.from_data(data["acl"])

        return d
