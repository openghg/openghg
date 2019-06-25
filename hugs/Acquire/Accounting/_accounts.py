
__all__ = ["Accounts"]


class Accounts:
    """This class provides the interface to grouping and ungrouping
       accounts, and associating them with users and services. An account
       can belong to multiple groups. Authorisation of access to accounts
       is based on which group they are in. Typically the groups will
       refer to users, e.g. all of the accounts for a user will be in the
       user's group, and can then be authorised by authorising the user
    """
    def __init__(self, user_guid=None, group=None, aclrules=None):
        """Construct an interface to the group of accounts called 'group'.
           If the group is not specified, then it will default to 'default'

           The passed ACLRules are used to control access to this group.
           Note that these control access to the group.
        """
        if group is None:
            if user_guid is None:
                group = "default"
            else:
                group = user_guid

        self._group = str(group)
        self._get_aclrules(user_guid=user_guid, aclrules=aclrules)
        self._user_guid = user_guid

    def __str__(self):
        return "Accounts(group=%s)" % self.group()

    def _root(self):
        """Return the root key for this group in the object store

            Returns:
                string: Root key for group
        """
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        return "accounting/account_groups/%s" % \
            _string_to_encoded(self._group)

    def _get_aclrules(self, user_guid, aclrules, bucket=None):
        """Load up the ACLRules for this group. If none are set, then
           either the passed ACLRules will be used, or the specified
           user will be set as the owner
        """
        from Acquire.Identity import ACLRules as _ACLRules
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        aclkey = self._acls_key()

        try:
            self._aclrules = _ACLRules.from_data(
                                _ObjectStore.get_object_from_json(
                                    bucket=bucket, key=aclkey))
        except:
            self._aclrules = None

        if self._aclrules is not None:
            return

        if aclrules is None:
            if user_guid is None:
                raise PermissionError(
                    "You must specify the guid of the initial user who "
                    "owns this account!")

            aclrules = _ACLRules.owner(user_guid=user_guid)
        elif not isinstance(aclrules, _ACLRules):
            raise TypeError("The ACLRules must be type ACLRules")

        _ObjectStore.set_object_from_json(bucket=bucket, key=aclkey,
                                          data=aclrules.to_data())

        self._aclrules = aclrules

    def identifiers(self):
        """Return the set of validated identifiers that are known to
           be attached to this group of accounts
        """
        return {"user_guid": self._user_guid}

    def aclrules(self):
        """Return the ACL rules for this group of accounts"""
        return self._aclrules

    def _account_key(self, name):
        """Return the key for the account called 'name' in this group

            Args:
                name (str): Name of the account

            Returns:
                str: Key for the account 'name'
        """
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        return "%s/%s" % (self._root(),
                          _string_to_encoded(str(name)))

    def _acls_key(self):
        """Return the key for the ACLs for this accounts group"""
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        return "accounting/account_group_acls/%s" % \
            _string_to_encoded(self._group)

    def group(self):
        """Return the name of the group that this set of accounts refers to

            Returns:
                string: Name of the group
        """
        return self._group

    def name(self):
        """Return the name of the group that this set of accounts refers to"""
        return self._group

    def _assert_is_readable(self):
        """Assert that we have permission to read these accounts"""
        aclrule = self._aclrules.resolve(must_resolve=True,
                                         identifiers=self.identifiers())

        if not aclrule.is_readable():
            raise PermissionError(
                "You do not have permission to access the accounts "
                "in this group")

    def _assert_is_writeable(self):
        """Assert that we have permission to write these accounts"""
        aclrule = self._aclrules.resolve(must_resolve=True,
                                         identifiers=self.identifiers())

        if not aclrule.is_writeable():
            raise PermissionError(
                "You do not have permission to write to the accounts "
                "in this group")

    def list_accounts(self, bucket=None):
        """Return the names of all of the accounts in this group

            Args:
                bucket (dict, default=None): Bucket from which to load data

            Returns:
                :obj:`list`: List of names of the accounts in this group

        """
        self._assert_is_readable()

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import encoded_to_string \
            as _encoded_to_string

        keys = _ObjectStore.get_all_object_names(bucket, self._root())
        root_len = len(self._root())

        accounts = []

        for key in keys:
            try:
                account_key = key[root_len:]
                while account_key.startswith("/"):
                    account_key = account_key[1:]

                accounts.append(_encoded_to_string(account_key))
            except Exception as e:
                from Acquire.Accounting import AccountError
                raise AccountError(
                    "Unable to identify the account associated with key "
                    "'%s', equals '%s': %s" %
                    (key, account_key, str(e)))

        return accounts

    def get_account(self, name, bucket=None):
        """Return the account called 'name' from this group

            Args:
                name (:obj:`str`): Name of account to retrieve
                bucket (:obj:`dict`): Bucket to load data from

            Returns:
                :obj:`Account`: Account object
        """
        self._assert_is_readable()

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        try:
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            account_uid = _ObjectStore.get_string_object(
                            bucket, self._account_key(name))
        except:
            account_uid = None

        if account_uid is None:
            # ensure that the user always has a "main" account
            if name == "main":
                return self.create_account("main", "primary user account",
                                           overdraft_limit=0, bucket=bucket)

            from Acquire.Accounting import AccountError
            raise AccountError("There is no account called '%s' in the "
                               "group '%s'" % (name, self.group()))

        from Acquire.Accounting import Account as _Account
        return _Account(uid=account_uid, bucket=bucket)

    def contains(self, account, bucket=None):
        """Return whether or not this group contains the passed account

            Args:
                account (:obj:`Account`): Account to check against group
                bucket (dict, default=None): Bucket to load data from
            Returns:
                bool : True if account in group, else False
        """
        self._assert_is_readable()

        from Acquire.Accounting import Account as _Account
        if not isinstance(account, _Account):
            raise TypeError("The passed account must be of type Account")

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        # read the UID of the account in this group that matches the
        # passed account's name
        try:
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            account_uid = _ObjectStore.get_string_object(
                            bucket, self._account_key(account.name()))
        except:
            account_uid = None

        return account.uid() == account_uid

    def create_account(self, name, description=None,
                       overdraft_limit=None, bucket=None,
                       authorisation=None):
        """Create a new account called 'name' in this group. This will
           return the existing account if it already exists

           Args:
                name (str): Name of account to create
                description (default=None): Description of account
                overdraft_limit (int, default=None): Limit of overdraft
                bucket (dict, default=None): Bucket to load data from

            Returns:
                Account: New Account object
        """
        if name is None:
            raise ValueError("You must pass a name of the new account")

        from Acquire.Identity import Authorisation as _Authorisation

        if authorisation is not None:
            if not isinstance(authorisation, _Authorisation):
                raise TypeError("The authorisation must be type Authorisation")

            if self._user_guid is not None:
                if self._user_guid != authorisation.user_guid():
                    raise PermissionError(
                        "The same user who opened this accounts group (%s) "
                        "must create accounts in this group (%s)" %
                        (self._user_guid, authorisation.user_guid()))

            authorisation.verify("create_account %s" % name)

        self._assert_is_writeable()

        account_key = self._account_key(name)

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Accounting import Account as _Account

        try:
            account_uid = _ObjectStore.get_string_object(bucket, account_key)
        except:
            account_uid = None

        if account_uid is not None:
            # this account already exists - just return it
            account = _Account(uid=account_uid, bucket=bucket)

            if account.group_name() != self.name():
                account.set_group(self)

            if overdraft_limit is not None:
                account.set_overdraft_limit(overdraft_limit, bucket=bucket)

            return account

        # make sure that no-one has created this account before
        from Acquire.ObjectStore import Mutex as _Mutex
        m = _Mutex(account_key, timeout=600, lease_time=600, bucket=bucket)

        try:
            account_uid = _ObjectStore.get_string_object(bucket, account_key)
        except:
            account_uid = None

        if account_uid is not None:
            m.unlock()
            # this account already exists - just return it
            account = _Account(uid=account_uid, bucket=bucket)

            if account.group_name() != self.name():
                account.set_group(self)

            if overdraft_limit is not None:
                account.set_overdraft_limit(overdraft_limit, bucket=bucket)

            return account

        # write a temporary UID to the object store so that we
        # can ensure we are the only function to create it
        try:
            _ObjectStore.set_string_object(bucket, account_key,
                                           "under_construction")
        except:
            m.unlock()
            raise

        m.unlock()

        # ok - we are the only function creating this account. Let's try
        # to create it properly. The account is created with the same
        # ACLRules of the group.
        try:
            from Acquire.Identity import ACLRules as _ACLRules
            account = _Account(name=name, description=description,
                               group_name=self.name(),
                               aclrules=_ACLRules.inherit(),
                               bucket=bucket)
        except:
            try:
                _ObjectStore.delete_object(bucket, account_key)
            except:
                pass

            raise

        if overdraft_limit is not None:
            account.set_overdraft_limit(overdraft_limit, bucket=bucket)

        _ObjectStore.set_string_object(bucket, account_key, account.uid())

        return account
