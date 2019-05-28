

__all__ = ["ACLRule"]


def _max(a, b):
    """Function to do most-permissive addition of two bools

        True + True = True
        True + False = True
        False + True = True
        False + False = False

        True + None = True
        None + True = True

        False + None = False
        None + False = False

        None + None = None
    """
    if a is None:
        return b
    elif b is None:
        return a
    elif a is True:
        return True
    else:
        return b


def _sub(a, b):
    """Function to do subtraction of two bools

        True - True = False
        True - False = True
        False - True = False
        False - False = False

        True - None = True
        None - True = False

        False - None = False
        None - False = None

        None - None = None
    """
    if b is True:
        return False
    else:
        return a


def _min(a, b):
    """Function to do least-permission addition of two bools

        True * True = True
        True * False = False
        False * True = False
        False * False = False

        True * None = True
        None * True = True

        False * None = False
        None * False = False

        None - None = None
    """
    if a is None:
        return b
    elif b is None:
        return a
    elif a is False:
        return False
    else:
        return b


class ACLRule:
    """This class holds the access control list (ACL) rule for
       a particular user accessing a particular resource
    """
    def __init__(self, is_owner=None, is_readable=None,
                 is_writeable=None, is_executable=None):
        """Construct a default rule. By default this rule has
           fully-inherit permissions (nothing is set either way)
        """
        if is_owner is None:
            self._is_owner = None
        elif is_owner:
            self._is_owner = True
        else:
            self._is_owner = False

        if is_readable is None:
            self._is_readable = None
        elif is_readable:
            self._is_readable = True
        else:
            self._is_readable = False

        if is_writeable is None:
            self._is_writeable = None
        elif is_writeable:
            self._is_writeable = True
        else:
            self._is_writeable = False

        if is_executable is None:
            self._is_executable = None
        elif is_executable:
            self._is_executable = True
        else:
            self._is_executable = False

    def __str__(self):
        s = []

        if self.is_owner():
            s.append("owner")
        elif self.inherits_owner():
            s.append("inherits_owner")

        if self.is_writeable():
            s.append("writeable")
        elif self.inherits_writeable():
            s.append("inherits_writeable")

        if self.is_readable():
            s.append("readable")
        elif self.inherits_readable():
            s.append("inherits_readable")

        if self.is_executable():
            s.append("executable")
        elif self.inherits_executable():
            s.append("inherits_executable")

        if len(s) == 0:
            return "ACLRule::denied"
        else:
            return "ACLRule(%s)" % ", ".join(s)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __add__(self, other):
        """Add two rules together - this will combine the parts
           additively, i.e. the most-permissive options are set
           in the return value
        """
        if not isinstance(other, ACLRule):
            raise TypeError(
                "You can only combine together pairs of ACLRules")

        return ACLRule(
                is_owner=_max(self._is_owner, other._is_owner),
                is_writeable=_max(self._is_writeable, other._is_writeable),
                is_readable=_max(self._is_readable, other._is_readable),
                is_executable=_max(self._is_executable, other._is_executable))

    def __sub__(self, other):
        """Subtract 'other' from these rules - if 'other' is False
           the this is set to False
        """
        if not isinstance(other, ACLRule):
            raise TypeError(
                "You can only combine together pairs of ACLRules")

        return ACLRule(
                is_owner=_sub(self._is_owner, other._is_owner),
                is_writeable=_sub(self._is_writeable, other._is_writeable),
                is_readable=_sub(self._is_readable, other._is_readable),
                is_executable=_sub(self._is_executable, other._is_executable))

    def __mul__(self, other):
        """Combine two rules together - this will combine
           the parts subtractively, i.e. the least permissive options
           are set in the return value
        """
        if not isinstance(other, ACLRule):
            raise TypeError(
                "You can only combine together pairs of ACLRules")

        return ACLRule(
                is_owner=_min(self._is_owner, other._is_owner),
                is_writeable=_min(self._is_writeable, other._is_writeable),
                is_readable=_min(self._is_readable, other._is_readable),
                is_executable=_min(self._is_executable, other._is_executable))

    @staticmethod
    def owner():
        """Return the ACLRule of an owner"""
        return ACLRule(is_owner=True, is_readable=True,
                       is_writeable=True, is_executable=True)

    @staticmethod
    def executer():
        """Return the ACLRule of an executer"""
        return ACLRule(is_owner=False, is_executable=True,
                       is_writeable=True, is_readable=True)

    @staticmethod
    def writer():
        """Return the ACLRule of a writer"""
        return ACLRule(is_owner=False, is_readable=True,
                       is_writeable=True, is_executable=False)

    @staticmethod
    def reader():
        """Return the ACLRule of a reader"""
        return ACLRule(is_owner=False, is_readable=True,
                       is_writeable=False, is_executable=False)

    @staticmethod
    def denied():
        """Return a "denied" (no-access at all) permission rule"""
        return ACLRule(is_owner=False, is_readable=False,
                       is_writeable=False, is_executable=False)

    @staticmethod
    def null():
        """Return a null (inherit all permissions) rule"""
        return ACLRule(is_owner=None, is_readable=None,
                       is_writeable=None, is_executable=None)

    @staticmethod
    def inherit():
        """Return the ACL rule that means 'inherit permissions from parent'"""
        return ACLRule(is_owner=None, is_readable=None,
                       is_writeable=None, is_executable=None)

    def is_owner(self):
        """Return whether or not the user is the owner of this resource"""
        return self._is_owner

    def is_readable(self):
        """Return whether or not the user can read this resource"""
        return self._is_readable

    def is_writeable(self):
        """Return whether or not the user can write to this resource"""
        return self._is_writeable

    def is_executable(self):
        """Return whether or not the user can execute this resource"""
        return self._is_executable

    def inherits_owner(self):
        """Return whether or not this inherits the owner status
           from upstream
        """
        return self._is_owner is None

    def inherits_readable(self):
        """Return whether or not this inherits the reader status
           from upstream
        """
        return self._is_readable is None

    def inherits_writeable(self):
        """Return whether or not this inherits the writeable status
           from upstream
        """
        return self._is_writeable is None

    def inherits_executable(self):
        """Return whether or not this inherits the executable status
           from upstream
        """
        return self._is_executable is None

    def inherits_all(self):
        """Return whether or not this rule inherits all permissions"""
        return (self._is_owner is None) and \
               (self._is_readable is None) and \
               (self._is_writeable is None) and \
               (self._is_executable is None)

    def is_fully_resolved(self):
        """Return whether or not this rule is fully resolved"""
        return (self._is_owner is not None) and \
               (self._is_readable is not None) and \
               (self._is_writeable is not None) and \
               (self._is_executable is not None)

    def denied_all(self):
        """Return whether or not this rule shows that everything is
           denied
        """
        return (self._is_owner is False) and \
               (self._is_readable is False) and \
               (self._is_writeable is False) and \
               (self._is_executable is False)

    def _force_resolve(self, unresolved=False):
        """This returns a fully resolved ACL, where anything that
           is not resolved is set equal to 'unresolved'
        """
        if unresolved:
            unresolved = True
        else:
            unresolved = False

        is_owner = self._is_owner
        is_readable = self._is_readable
        is_writeable = self._is_writeable
        is_executable = self._is_executable

        if is_owner is None:
            is_owner = unresolved

        if is_readable is None:
            is_readable = unresolved

        if is_executable is None:
            is_executable = unresolved

        if is_writeable is None:
            is_writeable = unresolved

        return ACLRule(is_owner=is_owner, is_writeable=is_writeable,
                       is_readable=is_readable, is_executable=is_executable)

    def resolve(self, must_resolve=True, identifiers=None,
                upstream=None, unresolved=False):
        """Resolve these rules based on the information supplied
           in 'identifiers'. Notably, if any of our rules are 'inherit',
           the this will look into 'upstream' to
           inherit the rule. If 'must_resolve' is true, then
           this function must always return a fully-resolved ACLRule
           (in which any unresolved parts are set equal to 'unresolved')
        """
        if self.is_fully_resolved():
            return self

        if upstream is None:
            if must_resolve:
                return self._force_resolve(unresolved=unresolved)
            else:
                return self

        from Acquire.Identity import ACLRules as _ACLRules
        if isinstance(upstream, _ACLRules):
            upstream = upstream.resolve(must_resolve=False,
                                        identifiers=identifiers)

        if must_resolve and (not upstream.is_fully_resolved()):
            upstream = upstream._force_resolve(unresolved=unresolved)

        is_owner = self._is_owner
        is_readable = self._is_readable
        is_writeable = self._is_writeable
        is_executable = self._is_executable

        if is_owner is None:
            is_owner = upstream._is_owner

        if is_readable is None:
            is_readable = upstream._is_readable

        if is_writeable is None:
            is_writeable = upstream._is_writeable

        if is_executable is None:
            is_executable = upstream._is_executable

        result = ACLRule(is_owner=is_owner, is_writeable=is_writeable,
                         is_readable=is_readable, is_executable=is_executable)

        if must_resolve:
            result = result._force_resolve(unresolved=unresolved)

        return result

    def set_owner(self, is_owner=True):
        """Set the user as an owner of the bucket"""
        if is_owner:
            self._is_owner = True
        else:
            self._is_owner = False

    def set_readable(self, is_readable=True):
        """Set the readable rule to 'is_readable'"""
        if is_readable:
            self._is_readable = True
        else:
            self._is_readable = False

    def set_writeable(self, is_writeable=True):
        """Set the writeable rule to 'is_writeable'"""
        if is_writeable:
            self._is_writeable = True
        else:
            self._is_writeable = False

    def set_inherits_owner(self):
        """Set that this ACL inherits ownership from its parent"""
        self._is_owner = None

    def set_inherits_readable(self):
        """Set that this ACL inherits readable from its parent"""
        self._is_readable = None

    def set_inherits_writeable(self):
        """Set that this ACL inherits writeable from its parent"""
        self._is_writeable = None

    def set_readable_writeable(self, is_readable_writeable=True):
        """Set both the readable and writeable rules to
           'is_readable_writeable'
        """
        if is_readable_writeable:
            self._is_readable = True
            self._is_writeable = True
        else:
            self._is_readable = False
            self._is_writeable = False

    def to_data(self):
        """Return this object converted to a json-serialisable object"""
        if self.inherits_all():
            return "inherits"
        elif self == ACLRule.owner():
            return "owner"
        elif self == ACLRule.reader():
            return "reader"
        elif self == ACLRule.writer():
            return "writer"
        elif self == ACLRule.denied():
            return "denied"
        elif self == ACLRule.executer():
            return "executer"
        else:
            data = {}
            data["is_owner"] = self._is_owner
            data["is_readable"] = self._is_readable
            data["is_writeable"] = self._is_writeable
            data["is_executable"] = self._is_executable
            return data

    @staticmethod
    def from_data(data):
        """Return this object constructed from the passed json-deserialised
           dictionary
        """
        if data is None:
            return None

        if isinstance(data, str):
            if data == "inherits":
                return ACLRule.inherit()
            elif data == "owner":
                return ACLRule.owner()
            elif data == "reader":
                return ACLRule.reader()
            elif data == "writer":
                return ACLRule.writer()
            elif data == "executer":
                return ACLRule.executer()
            elif data == "denied":
                return ACLRule.denied()

        is_owner = None
        is_readable = None
        is_writeable = None

        if "is_owner" in data:
            if data["is_owner"]:
                is_owner = True
            elif data["is_owner"] is not None:
                is_owner = False

        if "is_readable" in data:
            if data["is_readable"]:
                is_readable = True
            elif data["is_readable"] is not None:
                is_readable = False

        if "is_writeable" in data:
            if data["is_writeable"]:
                is_writeable = True
            elif data["is_writeable"] is not None:
                is_writeable = False

        if "is_executable" in data:
            if data["is_executable"]:
                is_executable = True
            elif data["is_executable"] is not None:
                is_executable = False

        return ACLRule(is_owner=is_owner,
                       is_writeable=is_writeable,
                       is_readable=is_readable,
                       is_executable=is_executable)
