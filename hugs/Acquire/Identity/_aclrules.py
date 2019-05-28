
from enum import Enum as _Enum

__all__ = ["ACLRules", "ACLUserRules", "ACLGroupRules"]


class ACLRuleOperation(_Enum):
    MAX = "max"  # add rules together (most permissive)
    MIN = "min"  # add rules together (least permissive)
    SUB = "sub"  # subtract rules (why?)
    SET = "set"  # break - set first matching fully-resolved rule

    def to_data(self):
        return self.value

    def combine(self, acl1, acl2):
        if acl1 is None:
            return acl2
        elif acl2 is None:
            return acl1

        if self is ACLRuleOperation.SET:
            return acl1
        elif self is ACLRuleOperation.MAX:
            return acl1 + acl2
        elif self is ACLRuleOperation.MIN:
            return acl1 * acl2
        elif self is ACLRuleOperation.SUB:
            return acl1 - acl2
        else:
            return None

    @staticmethod
    def from_data(data):
        return ACLRuleOperation(data)


def _save_rule(rule):
    """Return a json-serialisable object for the passed rule"""
    return [rule.__class__.__name__, rule.to_data()]


def _load_rule(rule):
    """Return the rule loaded from the json-deserialised data"""
    try:
        classname = rule[0]
        classdata = rule[1]
    except:
        raise TypeError("Expected [classname, classdata]")

    if classname == "ACLRules":
        return ACLRules.from_data(classdata)
    elif classname == "ACLUserRules":
        return ACLUserRules.from_data(classdata)
    elif classname == "ACLGroupRules":
        return ACLGroupRules.from_data(classdata)
    elif classname == "ACLRule":
        from Acquire.Identity import ACLRule as _ACLRule
        return _ACLRule.from_data(classdata)
    else:
        raise TypeError("Unrecognised type '%s'" % classname)


class ACLGroupRules:
    """This class holds rules that apply to individual groups"""
    def __init__(self):
        """Construct, optionally starting with a default rule
           is no groups are matched
        """
        self._group_rules = {}

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __str__(self):
        s = []
        for group, rule in self._group_rules.items():
            s.append("%s => %s" % (group, rule))
        return "Group{%s}" % ", ".join(s)

    def __repr__(self):
        return self.__str__()

    def resolve(self, identifiers=None, must_resolve=True,
                upstream=None, unresolved=False):
        """Resolve the rule for the user with specified group_guid.
           This returns None if there are no rules for this group
        """
        try:
            group_guids = identifiers["group_guids"]
        except:
            group_guids = []

        try:
            group_guids.append(identifiers["group_guid"])
        except:
            pass

        resolved = None

        for group_guid in group_guids:
            if group_guid in self._group_rules:
                rule = self._group_rules[group_guid]
                rule.resolve(must_resolve=must_resolve,
                             identifers=identifiers,
                             upstream=upstream,
                             unresolved=unresolved)
                if resolved is None:
                    resolved = rule
                else:
                    resolved = resolved + rule

        if resolved is None:
            if must_resolve:
                from Acquire.Identity import ACLRule as _ACLRule
                return _ACLRule.inherit().resolve(must_resolve=True,
                                                  identifiers=identifiers,
                                                  upstream=upstream,
                                                  unresolved=unresolved)
            else:
                return None
        else:
            return resolved

    def add_group_rule(self, group_guid, rule):
        """Add a rule for the used with passed 'group_guid'"""
        self._group_rules[group_guid] = rule

    def to_data(self):
        """Return a json-serialisable representation of these rules"""
        data = {}

        for group_guid, rule in self._group_rules.items():
            data[group_guid] = _save_rule(rule)

        return data

    @staticmethod
    def from_data(data):
        """Return the rules constructed from the passed json-deserialised
           object
        """
        rules = ACLGroupRules()

        if data is not None and len(data) > 0:
            for group_guid, rule in data.items():
                rules.add_group_rule(group_guid, _load_rule(rule))

        return rules


class ACLUserRules:
    """This class holds rules that apply to individual users"""
    def __init__(self):
        """Construct, optionally starting with a default rule
           if no users are matched
        """
        self._user_rules = {}

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __str__(self):
        s = []
        for user, rule in self._user_rules.items():
            s.append("%s => %s" % (user, rule))
        return "User{%s}" % ", ".join(s)

    def resolve(self, must_resolve=True, identifiers=None,
                upstream=None, unresolved=False):
        """Resolve the rule for the user with specified user_guid.
           This returns None if there are no rules for this user
        """
        try:
            user_guids = identifiers["user_guids"]
        except:
            user_guids = []

        try:
            user_guids.append(identifiers["user_guid"])
        except:
            pass

        resolved = None

        for user_guid in user_guids:
            if user_guid in self._user_rules:
                rule = self._user_rules[user_guid]
                rule.resolve(must_resolve=must_resolve,
                             identifiers=identifiers,
                             upstream=upstream,
                             unresolved=unresolved)
                if resolved is None:
                    resolved = rule
                else:
                    resolved = resolved + rule

        if resolved is None:
            if must_resolve:
                from Acquire.Identity import ACLRule as _ACLRule
                return _ACLRule.inherit().resolve(must_resolve=True,
                                                  identifiers=identifiers,
                                                  upstream=upstream,
                                                  unresolved=unresolved)
            else:
                return None
        else:
            return resolved

    def add_user_rule(self, user_guid, rule):
        """Add a rule for the used with passed 'user_guid'"""
        self._user_rules[user_guid] = rule

    @staticmethod
    def _create(aclrule, user_guid, user_guids):
        rule = ACLUserRules()

        if user_guid is not None:
            rule.add_user_rule(user_guid, aclrule)

        if user_guids is not None:
            for user_guid in user_guids:
                rule.add_user_rule(user_guid, aclrule)

        return rule

    @staticmethod
    def owner(user_guid=None, user_guids=None):
        """Simple shorthand to create the rule that the specified
           user is the owner of the resource
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLUserRules._create(aclrule=_ACLRule.owner(),
                                    user_guid=user_guid,
                                    user_guids=user_guids)

    @staticmethod
    def executer(user_guid=None, user_guids=None):
        """Simple shorthand to create the rule that the specified
           user is the executer of the resource
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLUserRules._create(aclrule=_ACLRule.executer(),
                                    user_guid=user_guid,
                                    user_guids=user_guids)

    @staticmethod
    def writer(user_guid=None, user_guids=None):
        """Simple shorthand to create the rule that the specified
           user is the writer of the resource
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLUserRules._create(aclrule=_ACLRule.writer(),
                                    user_guid=user_guid,
                                    user_guids=user_guids)

    @staticmethod
    def reader(user_guid=None, user_guids=None):
        """Simple shorthand to create the rule that the specified
           user is the reader of the resource
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLUserRules._create(aclrule=_ACLRule.reader(),
                                    user_guid=user_guid,
                                    user_guids=user_guids)

    def to_data(self):
        """Return a json-serialisable representation of these rules"""
        data = {}

        for user_guid, rule in self._user_rules.items():
            data[user_guid] = _save_rule(rule)

        return data

    @staticmethod
    def from_data(data):
        """Return the rules constructed from the passed json-deserialised
           object
        """
        rules = ACLUserRules()

        if data is not None and len(data) > 0:
            for user_guid, rule in data.items():
                rules.add_user_rule(user_guid, _load_rule(rule))

        return rules


def _is_inherit(aclrule):
    """Return whether or not this passed rule is just an inherit-all"""
    from Acquire.Identity import ACLRule as _ACLRule

    if isinstance(aclrule, _ACLRule):
        if aclrule == _ACLRule.inherit():
            return True

    return False


class ACLRules:
    """This class holds a combination of ACL rules. These are parsed
       in order to get the ACL for a resource.

       By default, this is a simple inherit rule (meaning that
       it will inherit whatever comes from upstream)
    """
    def __init__(self, rule=None, rules=None, default_rule=None,
                 default_operation=ACLRuleOperation.MAX):
        """Construct, optionally starting with a default ACLRule
           for all users
        """
        if default_rule is None:
            self._is_simple_inherit = True
        elif _is_inherit(default_rule):
            self._is_simple_inherit = True
        else:
            self._is_simple_inherit = False
            self._default_rule = default_rule
            self._rules = []

        self.set_default_operation(default_operation)

        if rule is not None:
            self.prepend(rule)

        if rules is not None:
            for rule in rules:
                try:
                    aclrule = rule[1]
                    oper = rule[0]
                except:
                    aclrule = rule
                    oper = default_operation

                self.append(aclrule=aclrule, operation=oper)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __str__(self):
        if self._is_simple_inherit:
            return "inherit"

        s = []
        for rule in self._rules:
            s.append("%s" % rule)

        if self._default_rule is not None:
            s.append("DEFAULT %s" % self._default_rule)

        return "ACLRules{\n%s\n}" % "\n".join(s)

    @staticmethod
    def _create(rule, user_guid=None, group_guid=None,
                user_guids=None, group_guids=None,
                default_rule=None):
        """Create and return the ACLRules that applies 'rules' to
           everyone specified (or to everyone, if this is not
           specified)
        """
        from Acquire.Identity import ACLRule as _ACLRule

        if default_rule is not None:
            if not isinstance(default_rule, _ACLRule):
                raise TypeError("The default_rule must be type ACLRule")

        if user_guids is None:
            user_guids = []

        if group_guids is None:
            group_guids = []

        if user_guid is not None:
            user_guids.append(user_guid)

        if group_guid is not None:
            group_guids.append(group_guid)

        if len(user_guids) == 0 and len(group_guids) == 0:
            if isinstance(rule, ACLRules):
                # this is a copy constructor
                return rule
            else:
                # no users or groups are specified - rule applies to everyone
                return ACLRules(default_rule=rule)

        if default_rule is None:
            default_rule = _ACLRule.denied()

        rules = []

        if len(group_guids) > 0:
            group_rules = ACLGroupRules()
            group_rules.add_group_rule(group_guid, rule)
            rules.append(group_rules)

        if len(user_guids) > 0:
            user_rules = ACLUserRules()
            user_rules.add_user_rule(user_guid, rule)
            rules.append(user_rules)

        return ACLRules(rules=rules, default_rule=default_rule)

    @staticmethod
    def create(rule, user_guid=None, user_guids=None,
               group_guid=None, group_guids=None,
               default_rule=None):
        """Create and return the ACLRules that gives 'rule' to
           everyone specified (or to everyone, if this is not
           specified)
        """
        return ACLRules._create(user_guid=user_guid,
                                user_guids=user_guids,
                                group_guid=group_guid,
                                group_guids=group_guids,
                                default_rule=default_rule,
                                rule=rule)

    @staticmethod
    def owner(user_guid=None, user_guids=None,
              group_guid=None, group_guids=None,
              default_rule=None):
        """Create and return the ACLRules that gives ownership to
           everyone specified (or to everyone, if this is not
           specified)
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLRules._create(user_guid=user_guid,
                                user_guids=user_guids,
                                group_guid=group_guid,
                                group_guids=group_guids,
                                default_rule=default_rule,
                                rule=_ACLRule.owner())

    @staticmethod
    def reader(user_guid=None, user_guids=None,
               group_guid=None, group_guids=None,
               default_rule=None):
        """Create and return the ACLRules that gives readership to
           everyone specified (or to everyone, if this is not
           specified)
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLRules._create(user_guid=user_guid,
                                user_guids=user_guids,
                                group_guid=group_guid,
                                group_guids=group_guids,
                                default_rule=default_rule,
                                rule=_ACLRule.reader())

    @staticmethod
    def writer(user_guid=None, user_guids=None,
               group_guid=None, group_guids=None,
               default_rule=None):
        """Create and return the ACLRules that gives writership to
           everyone specified (or to everyone, if this is not
           specified)
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLRules._create(user_guid=user_guid,
                                user_guids=user_guids,
                                group_guid=group_guid,
                                group_guids=group_guids,
                                default_rule=default_rule,
                                rule=_ACLRule.writer())

    @staticmethod
    def inherit(user_guid=None, user_guids=None,
                group_guid=None, group_guids=None,
                default_rule=None):
        """Create and return the ACLRules that sets inherit to
           everyone specified (or to everyone, if this is not
           specified)
        """
        from Acquire.Identity import ACLRule as _ACLRule
        return ACLRules._create(user_guid=user_guid,
                                user_guids=user_guids,
                                group_guid=group_guid,
                                group_guids=group_guids,
                                default_rule=default_rule,
                                rule=_ACLRule.inherit())

    def is_simple_inherit(self):
        """Return whether or not this set of rules is a simple
           'inherit all'
        """
        return self._is_simple_inherit

    def set_default_operation(self, default_operation):
        """Set the default operation used to combine together rules"""
        if not isinstance(default_operation, ACLRuleOperation):
            raise TypeError(
                "The default operation must be type ACLRuleOperation")

        self._default_operation = default_operation

    def set_default_rule(self, aclrule):
        """Set the default rule if nothing else matches (optionally
           also specifying the default operation to combine rules)
        """
        if self._is_simple_inherit:
            if _is_inherit(aclrule):
                return
            else:
                self._is_simple_inherit = False
                self._default_rule = aclrule
                self._default_operation = ACLRuleOperation.MAX
                self._rules = []
        else:
            self._default_rule = aclrule

    def append(self, aclrule, operation=None, ensure_owner=False):
        """Append a rule onto the set of rules. This will resolve any
           conflicts in the rules. If 'ensure_owner' is True, then
           this will ensure that there is at least one user who
           has unambiguous ownership of the resource controlled by
           these ACL rules
        """
        try:
            idx = len(self._rules)
        except:
            idx = 2

        self.insert(idx=idx, aclrule=aclrule,
                    operation=operation, ensure_owner=ensure_owner)

    def prepend(self, aclrule, operation=None, ensure_owner=False):
        """Prepend a rule onto the set of rules"""
        self.insert(idx=0, aclrule=aclrule,
                    operation=operation, ensure_owner=ensure_owner)

    def insert(self, idx, aclrule, operation=None, ensure_owner=False):
        """Insert the passed rule at index 'idx', specifying the operation
           used to combine this rule with what has gone before
           (defaults to self._default_operation)
        """
        if operation is not None:
            if not isinstance(operation, ACLRuleOperation):
                raise TypeError(
                    "The ACL operation must be type ACLRuleOperation")

        if self._is_simple_inherit:
            if _is_inherit(aclrule):
                return
            else:
                from Acquire.Identity import ACLRule as _ACLRule
                self._is_simple_inherit = False
                self._default_rule = None
                self._rules = [_ACLRule.inherit()]

        if operation is not None:
            self._rules.insert(idx, (operation, aclrule))
        else:
            self._rules.insert(idx, aclrule)

        if ensure_owner:
            # need to write code to ensure there is at least one owner
            pass

    def rules(self):
        """Return the list of ACL rules that will be applied
           in order (including the default rule, if set)
        """
        if self._is_simple_inherit:
            from Acquire.Identity import ACLRule as _ACLRule
            return [(self._default_operation, _ACLRule.inherit())]
        else:
            import copy as copy
            r = copy.copy(self._rules)
            if self._default_rule is not None:
                r.append((self._default_operation, self._default_rule))

            return r

    def resolve(self, must_resolve=True, identifiers=None,
                upstream=None, unresolved=False):
        """Resolve the rule based on the passed identifiers. This will
           resolve the rules in order the final ACLRule has been
           generated. If 'must_resolve' is True, then
           this is guaranteed to return a fully-resolved simple ACLRule.
           Anything unresolved is looked up from 'upstream', or set
           equal to 'unresolved'
        """
        from Acquire.Identity import ACLRule as _ACLRule

        if self._is_simple_inherit:
            return _ACLRule.inherit().resolve(must_resolve=must_resolve,
                                              identifiers=identifiers,
                                              upstream=upstream,
                                              unresolved=unresolved)

        result = None
        must_break = False

        for rule in self._rules:
            if isinstance(rule, tuple):
                trule = tuple(rule)   # casting to stop linting error
                op = trule[0]
                rule = trule[1]
            else:
                op = self._default_operation

            # resolve the rule...
            rule = rule.resolve(must_resolve=False,
                                identifiers=identifiers,
                                upstream=upstream,
                                unresolved=unresolved)

            if rule is not None:
                if op is ACLRuleOperation.SET:
                    # take the first matching rule
                    result = rule
                    must_break = True
                    break
                elif result is None:
                    result = rule
                else:
                    result = op.combine(result, rule)

        if (not must_break) and (self._default_rule is not None):
            rule = self._default_rule.resolve(must_resolve=False,
                                              identifiers=identifiers,
                                              upstream=upstream,
                                              unresolved=unresolved)
            if result is None:
                result = rule
            else:
                result = self._default_operation.combine(result, rule)

        # should now have a fully resolved ACLRule...
        if result is None:
            return _ACLRule.denied()

        if not isinstance(result, _ACLRule):
            raise PermissionError(
                "Did not fully resolve the ACLRule - got %s" % str(result))

        if not result.is_fully_resolved():
            # we have not been able to generate a fully-resolved ACL
            result = result.resolve(must_resolve=True,
                                    identifiers=identifiers,
                                    upstream=upstream,
                                    unresolved=unresolved)

        return result

    @staticmethod
    def from_data(data):
        """Construct these rules from the passed json-serialised
           dictionary
        """
        if isinstance(data, str) and data == "inherit":
                return ACLRules()

        if data is not None and len(data) > 0:

            if "default_rule" in data:
                default_rule = _load_rule(data["default_rule"])
            else:
                default_rule = None

            if "default_operation" in data:
                default_operation = \
                    ACLRuleOperation.from_data(data["default_operation"])
            else:
                default_operation = ACLRuleOperation.MAX

            if "rules" in data:
                rules = []
                for rule in data["rules"]:
                    if isinstance(rule, dict):
                        aclrule = rule["rule"]
                        oper = rule["operation"]
                        rules.append((ACLRuleOperation.from_data(oper),
                                      _load_rule(aclrule)))
                    else:
                        rules.append((None, _load_rule(rule)))
            else:
                rules = None

            return ACLRules(default_rule=default_rule,
                            default_operation=default_operation,
                            rules=rules)
        else:
            return ACLRules()

    def to_data(self):
        """Return a json-serialisable dictionary of these rules"""
        if self._is_simple_inherit:
            return "inherit"

        data = {}

        if len(self._rules) == 1:
            rule = self._rules[0]
            if isinstance(rule, tuple):
                trule = tuple(rule)
                rule = trule[1]

            data["rules"] = [_save_rule(rule)]
        elif len(self._rules) > 1:
            rules = []

            for rule in self._rules:
                if isinstance(rule, tuple):
                    trule = tuple(rule)  # casting to stop linting error
                    if trule[0] is None:
                        rules.append(_save_rule(trule[1]))
                    else:
                        rules.append({"operation": trule[0].to_data(),
                                      "rule": _save_rule(trule[1])})
                else:
                    rules.append(_save_rule(rule))

            data["rules"] = rules

        if self._default_rule is not None:
            data["default_rule"] = _save_rule(self._default_rule)

        if self._default_operation is not ACLRuleOperation.MAX:
            data["default_operation"] = self._default_operation.to_data()

        return data
