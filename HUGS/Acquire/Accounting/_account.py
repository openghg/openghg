
__all__ = ["Account"]


def _account_root():
    return "accounting/accounts"


def _get_last_day(datetime):
    """Return the start of the day before 'datetime', e.g.
       _get_last_day(April 1st) will return March 31st
    """
    import datetime as _datetime
    datetime = datetime - _datetime.timedelta(days=1)
    return _datetime.datetime(year=datetime.year, month=datetime.month,
                              day=datetime.day, tzinfo=datetime.tzinfo)


def _get_last_month(datetime):
    """Return the date at the start of the month before 'datetime', e.g.
       _get_last_month(March 21st) will return February 1st
    """
    import datetime as _datetime
    datetime = datetime.replace(day=1) - _datetime.timedelta(days=1)
    return _datetime.datetime(year=datetime.year, month=datetime.month,
                              day=1, tzinfo=datetime.tzinfo)


def _get_hourly_datetime(datetime):
    """Return the datetime for the top of the hour of 'datetime',
       e.g. 5.42pm would return 5.00pm
    """
    from Acquire.ObjectStore import datetime_to_datetime \
        as _datetime_to_datetime
    import datetime as _datetime
    datetime = _datetime_to_datetime(datetime)
    return _datetime.datetime(year=datetime.year,
                              month=datetime.month,
                              day=datetime.day,
                              hour=datetime.hour,
                              tzinfo=datetime.tzinfo)


def _get_key_from_hour(start, datetime):
    """Return a key encoding the passed date, starting the key with 'start',
       but only up unto the specified hour
    """
    from Acquire.ObjectStore import datetime_to_datetime \
        as _datetime_to_datetime
    datetime = _datetime_to_datetime(datetime)
    return "%s/%sT%02d" % (start, datetime.date().isoformat(), datetime.hour)


def _get_key_from_day(start, datetime):
    """Return a key encoding the passed date, starting the key with 'start',
       but only up until the specified day
    """
    from Acquire.ObjectStore import datetime_to_datetime \
        as _datetime_to_datetime
    datetime = _datetime_to_datetime(datetime)
    return "%s/%4d-%02d-%02d" % (start, datetime.year, datetime.month,
                                 datetime.day)


def _get_key_from_month(start, datetime):
    """Return a key encoding the passed date, starting the key with 'start',
       but only up to the specified month
    """
    from Acquire.ObjectStore import datetime_to_datetime \
        as _datetime_to_datetime
    datetime = _datetime_to_datetime(datetime)
    return "%s/%4d-%02d" % (start, datetime.year, datetime.month)


def _get_hour_from_key(key):
    """Return the date that is encoded in the passed key"""
    import re as _re
    m = _re.search(r"(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d)", key)

    if m:
        from Acquire.ObjectStore import date_and_time_to_datetime \
            as _date_and_time_to_datetime
        import datetime as _datetime

        return _date_and_time_to_datetime(
                    _datetime.date(year=int(m.groups()[0]),
                                   month=int(m.groups()[1]),
                                   day=int(m.groups()[2])),
                    _datetime.time(hour=int(m.groups()[3])))
    else:
        from Acquire.Accounting import AccountError
        raise AccountError("Could not find a date in the key '%s'" % key)


def _get_datetime_from_key(key):
    """Return the datetime that is encoded in the passed key

       Args:
            key(st: obj: `str`): Key to search for datetime
       Returns:
            datetime: detailed datetime object read from key
    """
    import re as _re
    m = _re.search(r"(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d+)",
                   key)

    if m:
        from Acquire.ObjectStore import date_and_time_to_datetime \
            as _datetime_to_datetime
        import datetime as _datetime

        return _datetime_to_datetime(
                    _datetime.datetime(year=int(m.groups()[0]),
                                       month=int(m.groups()[1]),
                                       day=int(m.groups()[2]),
                                       hour=int(m.groups()[3]),
                                       minute=int(m.groups()[4]),
                                       second=int(m.groups()[5]),
                                       microsecond=int(m.groups()[6])))
    else:
        from Acquire.Accounting import AccountError
        raise AccountError("Could not find a datetime in the key '%s'" % key)


def _sum_transactions(transactions):
    """Internal function that sums all of the transactions identified
    by the passed keys.  by the passed keys. This returns a tuple of
    (balance, liability, receivable, spent)

        Args:
            keys (:obj:`list`): List of keys to parse
        Returns:
            tuple (:obj:`Decimal`, Decimal, Decimal, Decimal): balance,
            liability, receivable, spent_today

    """
    from Acquire.Accounting import Balance as _Balance
    from Acquire.Accounting import TransactionInfo as _TransactionInfo

    balance = _Balance()

    for transaction in transactions:
        if not isinstance(transaction, _TransactionInfo):
            transaction = _TransactionInfo(transaction)

        balance = balance + transaction

    return balance


class Account:
    """This class represents a single account in the ledger. It has a balance,
       and a record of the set of transactions that have been applied.

       The account really holds two accounts: the liability account and
       actual capital account. We combine both together into a single
       account to ensure that updates occur atomically

       All data for this account is stored in the object store

       The account has a set of ACLRules that specify who can
       read and write to the account (writing implies has spend
       authority), and who owns the account (can change ACLRules)
    """
    def __init__(self, name=None, description=None, uid=None,
                 aclrules=None, group_name=None, bucket=None):
        """Construct the account. If 'uid' is specified, then load the account
           from the object store (so 'name' and 'description' should be "None")
           You can also supply the ACLRules that will be used to control
           access to this account. If these are not specified then
           ACLRules.inherit() will be used, with rules inherited from the
           Accounts group that contains this Account

            Args:
                name (:obj:`str`, default=None): Name on the account
                description (:obj:`str`, default=None): Description of account
                uid (UID): Unique ID for account, if used do not pass name or
                description
                bucket (dict): contains data for bucket

            Returns:
                None

        """
        self._name = None
        self._description = None
        self._last_update = {}
        self._uid = None
        self._group_name = None

        if uid is not None:
            self._uid = str(uid)
            bucket = self._get_account_bucket(bucket)
            self._load_account(bucket)

            if name:
                if name != self.name():
                    from Acquire.Accounting import AccountError
                    raise AccountError(
                        "This account name '%s' does not match what you "
                        "expect! '%s'" % (self.name(), name))

            if description:
                if description != self.description():
                    from Acquire.Accounting import AccountError
                    raise AccountError(
                        "This account description '%s' does not match what "
                        "you expect! '%s'" % (self.description(), description))

        elif name is not None:
            self._uid = None
            self._create_account(name=name, description=description,
                                 group_name=group_name, aclrules=aclrules,
                                 bucket=bucket)

    def is_null(self):
        """Return whether or not this is a null account"""
        return self._uid is None

    def _get_now(self, now=None):
        """Return the time of 'now' (or actual now if not passed)"""
        if now is None:
            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            return _get_datetime_now()
        else:
            from Acquire.ObjectStore import datetime_to_datetime \
                as _datetime_to_datetime
            return _datetime_to_datetime(now)

    def _get_account_bucket(self, bucket=None):
        """Return the bucket into which to write this account,
           or 'bucket' if it is not None
        """
        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            return _get_service_account_bucket()
        else:
            return bucket

    def __str__(self):
        if self._uid is None:
            return "Account::null"
        else:
            return "Account(%s|%s|%s)" % (self._name, self._description,
                                          self._uid)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def _create_account(self, name, description, group_name, aclrules,
                        bucket=None):
        """Create the account from scratch"""
        if name is None or description is None:
            from Acquire.Accounting import AccountError
            raise AccountError(
                "You must pass both a name and description to create a new "
                "account")

        if self._uid is not None:
            from Acquire.Accounting import AccountError
            raise AccountError("You cannot create an account twice!")

        from Acquire.Identity import ACLRules as _ACLRules
        if aclrules is None:
            aclrules = _ACLRules.inherit()
        elif not isinstance(aclrules, _ACLRules):
            raise TypeError("The aclrules must be type ACLRules")

        from Acquire.Accounting import create_decimal as _create_decimal
        from Acquire.ObjectStore import create_uuid as _create_uuid

        bucket = self._get_account_bucket(bucket)

        self._uid = _create_uuid()
        self._name = str(name)
        self._description = str(description)
        self._overdraft_limit = _create_decimal(0)
        self._maximum_daily_limit = 0
        self._aclrules = aclrules

        if group_name is None:
            self._group_name = None
        else:
            self._group_name = str(group_name)

        # make sure that this is saved to the object store
        self._save_account(bucket)

    def _get_transactions_between(self, start_datetime, end_datetime,
                                  bucket=None):
        """Return all of the object store keys for transactions in this
           account beteen 'start_datetime' and 'end_datetime' (inclusive, e.g.
           start_datetime < transaction <= end_datetime). This will return an
           empty list if there were no transactions in this time
        """
        # convert both times to UTC
        from Acquire.ObjectStore import datetime_to_datetime \
            as _datetime_to_datetime
        import datetime as _datetime

        if start_datetime is None or end_datetime is None:
            raise ValueError("NULL %s | %s" % (start_datetime, end_datetime))

        start_datetime = _datetime_to_datetime(start_datetime)
        end_datetime = _datetime_to_datetime(end_datetime)

        # get the day for each time
        start_day = start_datetime.toordinal()
        end_day = end_datetime.toordinal()

        if end_datetime.time() == _datetime.time():
            # this ends on midnight of the first day - do not
            # include this last day as nothing will match
            end_day -= 1

        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime
        from Acquire.ObjectStore import date_to_string as _date_to_string
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Accounting import TransactionInfo as _TransactionInfo

        bucket = self._get_account_bucket()

        num_days = end_day - start_day

        if num_days < 7:
            # sufficiently few days that a day-by-day search is enough
            transactions = []
            for day in range(start_day, end_day+1):
                day_date = _datetime.datetime.fromordinal(day)
                day_string = _date_to_string(day_date)

                prefix = "%s/%s" % (self._transactions_key(), day_string)

                try:
                    keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                             prefix=prefix)
                except:
                    keys = []

                for key in keys:
                    transaction = _TransactionInfo.from_key(key)
                    datetime = transaction.datetime()
                    if datetime > start_datetime and datetime <= end_datetime:
                        transactions.append(transaction)

            return transactions

        # elif num_days < 300:  Try a better algorithm for weeks and months

        else:
            # likely more than years - easier to just scan all transactions
            # on the account
            prefix = self._transactions_key()

            try:
                keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                         prefix=prefix)
            except:
                keys = []

            transactions = []

            for key in keys:
                transaction = _TransactionInfo.from_key(key)
                datetime = transaction.datetime()
                if datetime > start_datetime and datetime <= end_datetime:
                    transactions.append(transaction)

            return transactions

    def _get_balance_key(self, now=None):
        """Return the balance key for the passed time. This is the key
           into the object store of the object that holds the starting
           balance for the account on the hour of the passed datetime.
           If 'now' is None, then the key for actual now is returned
        """

        if self.is_null():
            return None
        else:
            return _get_key_from_hour(start=self._balance_key(),
                                      datetime=self._get_now(now))

    def _find_last_balance_key(self, now=None, bucket=None):
        """Return the key containing the last hourly balance update before
           'now' (defaults to actual now if not set)
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        now = self._get_now(now)
        bucket = self._get_account_bucket(bucket)
        start = self._balance_key()

        # look for any balance keys from today
        prefix = _get_key_from_day(start=start, datetime=now)
        try:
            keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                     prefix=prefix)
        except:
            keys = []

        if len(keys) > 0:
            keys.sort()
            return keys[-1]

        # look for any balance keys from yesterday
        # (we do this to stop big lookups for active accounts when
        #  we jump between months)
        now = _get_last_day(now)
        prefix = _get_key_from_day(start=start, datetime=now)
        try:
            keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                     prefix=prefix)
        except:
            keys = []

        if len(keys) > 0:
            keys.sort()
            return keys[-1]

        # look for any balance keys from this month
        prefix = _get_key_from_month(start=start, datetime=now)
        try:
            keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                     prefix=prefix)
        except:
            keys = []

        if len(keys) > 0:
            keys.sort()
            return keys[-1]

        for _ in range(0, 6):
            now = _get_last_month(now)
            prefix = _get_key_from_month(start=start, datetime=now)
            try:
                keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                         prefix=prefix)
            except:
                keys = []

            if len(keys) > 0:
                keys.sort()
                return keys[-1]

        # wow - no balance keys at all over the last 6 months. Look for
        # *any* balance keys
        try:
            keys = _ObjectStore.get_all_object_names(bucket=bucket,
                                                     prefix=prefix)
        except:
            keys = []

        if len(keys) > 0:
            keys.sort()
            # can only return the latest key before 'now'
            for i in range(len(keys)-1, 0, -1):
                key = keys[i]
                hourly_time = _get_hour_from_key(key)

                if hourly_time < now:
                    return key

        # no balance keys at all! Set a balance key for the beginning of time
        from Acquire.ObjectStore import datetime_to_datetime \
            as _datetime_to_datetime
        from Acquire.Accounting import Balance as _Balance
        import datetime as _datetime
        hourly_time = _datetime_to_datetime(_datetime.datetime.fromordinal(1))

        hourly_key = self._get_balance_key(now=hourly_time)
        hourly_balance = _Balance()
        _ObjectStore.set_object_from_json(bucket=bucket, key=hourly_key,
                                          data=hourly_balance.to_data())

        return hourly_key

    def _get_hourly_balance(self, now=None, bucket=None):
        """Calculate and return the balance at the top of the hour
           for 'now' (defaults to actually now if not specified)
        """
        now = self._get_now(now)
        hourly_key = self._get_balance_key(now)

        if hourly_key in self._last_update:
            return self._last_update[hourly_key]["hourly_balance"]

        from Acquire.Accounting import Balance as _Balance
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        bucket = self._get_account_bucket(bucket)

        try:
            data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                     key=hourly_key)
            hourly_balance = _Balance.from_data(data)
        except:
            hourly_balance = None

        hourly_now_time = _get_hourly_datetime(now)

        if hourly_balance is None:
            # look for the last balance key...
            last_balance_key = self._find_last_balance_key(now=now,
                                                           bucket=bucket)

            if last_balance_key is None:
                from Acquire.Accounting import AccountError
                raise AccountError(
                    "The first balance of the account %s has not been set?"
                    % str(self))

            data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                     key=last_balance_key)

            last_balance = _Balance.from_data(data)
            last_balance_time = _get_hour_from_key(last_balance_key)

            transactions = self._get_transactions_between(
                                        start_datetime=last_balance_time,
                                        end_datetime=hourly_now_time)

            total = _sum_transactions(transactions)

            hourly_balance = last_balance + total

        _ObjectStore.set_object_from_json(bucket=bucket,
                                          key=hourly_key,
                                          data=hourly_balance.to_data())

        self._last_update[hourly_key] = \
            {"hourly_balance": hourly_balance,
             "last_update_time": hourly_now_time,
             "last_update_balance": hourly_balance}

        return hourly_balance

    def balance(self, now=None, bucket=None):
        """Get the balance of the account at 'now' (defaults to actually now).
           This returns a Balance object for the balance, that includes
           (1) the current real balance of the account,
           neglecting any outstanding liabilities or accounts receivable,
           (2) the current total liabilities,
           and (3) the current total accounts receivable

           where 'liability' is the current total liabilities,
           where 'receivable' is the current total accounts receivable, and
           where 'spent_today' is how much has been spent today (from midnight
           until now)

           Args:
                bucket (dict, default=None): Bucket to use for calculations

            Returns:
                tuple (Decimal, Decimal, Decimal, Decimal): balance, liability,
                receivable, spent_today
        """
        now = self._get_now(now)
        bucket = self._get_account_bucket(bucket)

        # get the key to the hourly balance for now
        hourly_key = self._get_balance_key(now)

        try:
            hourly_update = self._last_update[hourly_key]
        except:
            hourly_update = None

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        bucket = self._get_account_bucket()

        if hourly_update is None:
            hourly_balance = self._get_hourly_balance(bucket=bucket,
                                                      now=now)
            last_update_time = _get_hourly_datetime(now)
            last_update_balance = hourly_balance
        else:
            hourly_balance = hourly_update["hourly_balance"]
            last_update_time = hourly_update["last_update_time"]
            last_update_balance = hourly_update["last_update_balance"]

        if last_update_time >= now:
            # the last update of this balance was in the future - go from
            # the current hour
            last_update_time = _get_hourly_datetime(now)
            last_update_balance = hourly_balance

        # next, get the transactions that have taken place since the last
        # update and sum them to get the current balance
        transactions = self._get_transactions_between(
                                 start_datetime=last_update_time,
                                 end_datetime=now, bucket=bucket)

        total = last_update_balance + _sum_transactions(transactions)

        self._last_update[hourly_key] = {"hourly_balance": hourly_balance,
                                         "last_update_time": now,
                                         "last_update_balance": total}

        return total

    def name(self):
        """Return the name of this account

            Returns:
                str or None: Name of account if account not null, else None


        """
        if self.is_null():
            return None

        return self._name

    def group_name(self):
        """Return the name of the Accounts group in which this
           account belongs. An Account can only exist in a single
           Accounts Group at a time
        """
        if self.is_null():
            return None

        return self._group_name

    def description(self):
        """Return the description of this account

            Returns:
                str or None: Description of account if account not null,
                else None
        """
        if self.is_null():
            return None

        return self._description

    def uid(self):
        """Return the UID for this account.

            Returns:
                str: UID

        """
        return self._uid

    def assert_valid_authorisation(self, authorisation, resource=None,
                                   accept_partial_match=False):
        """Assert that the passed authorisation is valid for this
           account

            Args:
                authorisation (Authorisation): authorisation
                object to be used for account

            Returns:
                None
        """
        if authorisation is None:
            raise PermissionError("You need to supply a valid authorisation!")

        from Acquire.Identity import Authorisation as _Authorisation
        if not isinstance(authorisation, _Authorisation):
            raise TypeError("The passed authorisation must be an "
                            "Authorisation")

        user_guid = None

        identifiers = None

        if not authorisation.is_null():
            identifiers = authorisation.verify(
                                 resource=resource,
                                 accept_partial_match=accept_partial_match,
                                 return_identifiers=True)
            user_guid = authorisation.user_guid()

        upstream = None

        if self.group_name() is not None:
            from Acquire.Accounting import Accounts as _Accounts
            group = _Accounts(user_guid=user_guid, group=self.group_name())
            upstream = group.aclrules().resolve(must_resolve=False,
                                                identifiers=identifiers)

        aclrule = self._aclrules.resolve(must_resolve=True,
                                         upstream=upstream,
                                         identifiers=identifiers)

        if not aclrule.is_writeable():
            raise PermissionError(
                "You do not have permission to write (draw funds) from "
                "this account!")

    def _get_safe_now(self):
        """This function returns the current time. It avoids dangerous
           times (when the system may be updating) by sleeping through
           those times (i.e. it will sleep from HH:59:58 until HH+1:00:01)
        """
        now = self._get_now()

        # don't allow any transactions in the last 2 seconds of the hour, as
        # we will sum up the day balance at the top of each hour, and
        # don't want to risk any late transactions from messing up the
        # accounting. We also don't want any transactions at exactly the
        # top of the hour in case they get missed out when getting
        # transactions with date ranges
        while (now.minute == 59 and now.second >= 58) or \
              (now.minute == 0 and now.second == 0 and now.microsecond < 10):
            import time as _time
            # sleep in quarter-second increments to minimise disruption
            _time.sleep(0.25)
            new_now = self._get_now()

            if new_now == now:
                # now time has passed - we are being tested, so let's
                # pretend that time has gone forwards
                import datetime as _datetime
                new_now = now + _datetime.timedelta(seconds=1)

            now = new_now

        return now

    def _credit_refund(self, debit_note, refund, bucket=None):
        """Credit the value of the passed 'refund' to this account. The
           refund must be for a previous completed debit, hence the
           original debitted value is returned to the account.

            Args:
                debit_note (DebitNote): Note to be used for
                refund
                refund (Refund): Refund holding value to be refunded
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime): Return the UID and current time
        """
        from Acquire.Accounting import Refund as _Refund
        from Acquire.Accounting import DebitNote as _DebitNote

        if not isinstance(refund, _Refund):
            raise TypeError("The passed refund must be a Refund")

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("The passed debit note must be a DebitNote")

        if refund.is_null():
            return

        if refund.value() != debit_note.value():
            raise ValueError("The refunded value does not match the value "
                             "of the debit note: %s versus %s" %
                             (refund.value(), debit_note.value()))

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        encoded_value = _TransactionInfo.encode(
                                        _TransactionCode.RECEIVED_REFUND,
                                        refund.value())

        # create a UID and datetime for this credit and record
        # it in the account
        now = self._get_safe_now()

        # and to create a key to find this credit later. The key is made
        # up from the iso format of the datetime of the credit
        # and a random string
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Accounting import LineItem as _LineItem
        from Acquire.ObjectStore import create_uuid as _create_uuid

        datetime_key = _datetime_to_string(now)
        uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

        item_key = "%s/%s/%s" % (self._transactions_key(), uid, encoded_value)
        l = _LineItem(debit_note.uid(), refund.authorisation())

        bucket = self._get_account_bucket()
        _ObjectStore.set_object_from_json(bucket, item_key, l.to_data())

        return (uid, now)

    def _debit_refund(self, refund, bucket=None):
        """Debit the value of the passed 'refund' from this account. The
           refund must be for a previous completed credit. There is a risk
           that this value has been spent, so this is one of the only
           functions that allows a balance to drop below an overdraft or
           other limit (as the refund should always succeed).

            Args:
                refund (Refund): Refund note to be processed
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime): UID and current time
        """
        from Acquire.Accounting import Refund as _Refund

        if not isinstance(refund, _Refund):
            raise TypeError("The passed refund must be a Refund")

        if refund.is_null():
            return

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        encoded_value = _TransactionInfo.encode(_TransactionCode.SENT_REFUND,
                                                refund.value())

        bucket = self._get_account_bucket()

        while True:
            # create a UID and datetime for this debit and record
            # it in the account
            now = self._get_safe_now()

            # and to create a key to find this debit later. The key is made
            # up from the date and  of the debit and a random string
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            from Acquire.Accounting import LineItem as _LineItem
            from Acquire.ObjectStore import create_uuid as _create_uuid

            datetime_key = _datetime_to_string(now)
            uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

            item_key = "%s/%s/%s" % (self._transactions_key(),
                                     uid, encoded_value)
            l = _LineItem(uid, refund.authorisation())

            now2 = self._get_safe_now()

            if now2.hour == now.hour:
                # we have not moved into the next hour
                break

        _ObjectStore.set_object_from_json(bucket, item_key, l.to_data())

        return (uid, now)

    def _credit_receipt(self, debit_note, receipt, bucket=None):
        """Credit the value of the passed 'receipt' to this account. The
           receipt must be for a previous provisional credit, hence the
           money is awaiting transfer from accounts receivable.

            Args:
                debit_note (DebitNote): Holds the value of the credit
                to be applied to the account, value must match that of receipt
                receipt (Receipt): Receipt holding the value of the credit
                that is to be applied to account
                TODO - improve bucket docs
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime): UID and current time
        """
        from Acquire.Accounting import Receipt as _Receipt
        from Acquire.Accounting import DebitNote as _DebitNote

        if not isinstance(receipt, _Receipt):
            raise TypeError("The passed receipt must be a Receipt")

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("The passed debit note must be a DebitNote")

        if receipt.is_null():
            return

        if receipt.receipted_value() != debit_note.value():
            raise ValueError("The receipted value does not match the value "
                             "of the debit note: %s versus %s" %
                             (receipt.receipted_value(), debit_note.value()))

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        encoded_value = _TransactionInfo.encode(
                                    _TransactionCode.SENT_RECEIPT,
                                    receipt.value(), receipt.receipted_value())

        bucket = self._get_account_bucket()

        while True:
            # create a UID and datetime for this credit and record
            # it in the account
            now = self._get_safe_now()

            # and to create a key to find this credit later. The key is made
            # up from the isoformat datetime of the credit and a random string
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            from Acquire.Accounting import LineItem as _LineItem
            from Acquire.ObjectStore import create_uuid as _create_uuid

            datetime_key = _datetime_to_string(now)
            uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

            item_key = "%s/%s/%s" % (self._transactions_key(),
                                     uid, encoded_value)
            l = _LineItem(debit_note.uid(), receipt.authorisation())

            now2 = self._get_safe_now()

            if now2.hour == now.hour:
                # we have not moved into another hour
                break

        _ObjectStore.set_object_from_json(bucket, item_key, l.to_data())

        return (uid, now)

    def _debit_receipt(self, receipt, bucket=None):
        """Debit the value of the passed 'receipt' from this account. The
           receipt must be for a previous provisional debit, hence
           the money should be available.

            Args:
                receipt (Receipt): holds the value
                to be debited from the account
                TODO - improve bucket docs
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime): UID and current time
        """
        from Acquire.Accounting import Receipt as _Receipt

        if not isinstance(receipt, _Receipt):
            raise TypeError("The passed receipt must be a Receipt")

        if receipt.is_null():
            return

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        encoded_value = _TransactionInfo.encode(
                                    _TransactionCode.RECEIVED_RECEIPT,
                                    receipt.value(), receipt.receipted_value())

        bucket = self._get_account_bucket()

        # create a UID and datetime for this debit and record
        # it in the account
        while True:
            now = self._get_safe_now()

            # and to create a key to find this debit later. The key is made
            # up from the isoformat datetime of the debit and a random string
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            from Acquire.Accounting import LineItem as _LineItem
            from Acquire.ObjectStore import create_uuid as _create_uuid

            datetime_key = _datetime_to_string(now)
            uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

            item_key = "%s/%s/%s" % (self._transactions_key(),
                                     uid, encoded_value)
            l = _LineItem(uid, receipt.authorisation())

            now2 = self._get_safe_now()

            if now2.hour == now.hour:
                # we are safely in the same hour
                break

        _ObjectStore.set_object_from_json(bucket, item_key, l.to_data())

        return (uid, now)

    def _credit(self, debit_note, bucket=None):
        """Credit the value in 'debit_note' to this account. If the debit_note
           shows that the payment is provisional then this will be recorded
           as accounts receivable. This will record the credit with the
           same UID as the debit identified in the debit_note, so that
           we can reconcile all credits against matching debits.

            Args:
                debit_note (DebitNote): Holds the value to be credited
                to this account
                TODO - improve bucket docs
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime): UID and current time
        """
        from Acquire.Accounting import DebitNote as _DebitNote

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("The passed debit note must be a DebitNote")

        if debit_note.value() <= 0:
            return

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        if debit_note.is_provisional():
            encoded_value = _TransactionInfo.encode(
                                _TransactionCode.ACCOUNT_RECEIVABLE,
                                debit_note.value())
        else:
            encoded_value = _TransactionInfo.encode(
                                _TransactionCode.CREDIT,
                                debit_note.value())

        bucket = self._get_account_bucket()

        # create a UID and datetime for this credit and record
        # it in the account
        while True:
            now = self._get_safe_now()

            # and to create a key to find this credit later. The key is made
            # up from the isoformat datetime of the credit and a random string
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            from Acquire.ObjectStore import ObjectStore as _ObjectStore
            from Acquire.Accounting import LineItem as _LineItem
            from Acquire.ObjectStore import create_uuid as _create_uuid

            datetime_key = _datetime_to_string(now)
            uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

            item_key = "%s/%s/%s" % (self._transactions_key(),
                                     uid, encoded_value)

            now2 = self._get_safe_now()

            if now2.hour == now.hour:
                # we are safely in the same hour
                break

        # the line item records the UID of the debit note, so we can
        # find this debit note in the system and, from this, get the
        # original transaction in the transaction record
        l = _LineItem(debit_note.uid(), debit_note.authorisation())

        _ObjectStore.set_object_from_json(bucket, item_key, l.to_data())

        return (uid, now)

    def _debit(self, transaction, authorisation,
               is_provisional, receipt_by,
               authorisation_resource=None, bucket=None):
        """Debit the value of the passed transaction from this account based
           on the authorisation contained
           in 'authorisation'. This will create a unique ID (UID) for
           this debit and will return this together with the datetime of the
           debit. If this transaction 'is_provisional' then it will be
           recorded as a liability, which must be receipted before
           'receipt_by'. If 'receipt_by' is None, then this will
           automatically be 1 week in the future

           The UID will encode both the date of the debit and provide a random
           ID that together can be used to identify the transaction associated
           with this debit in the future.

           This will raise an exception if the debit cannot be completed, e.g.
           if the authorisation is invalid, if the debit exceeds a limit or
           there are insufficient funds in the account

           Note that this function is private as it should only be called
           by the DebitNote class

            Args:
                transaction (Transaction): Holds the value to be debited
                from this account
                authorisation (Authorisation): Authorisation for the
                transaction
                is_provisional (bool): If True the transaction will be
                recorded as a liability
                receipt_by (datetime): Datetime by which the transaction
                should be receipted
                TODO - improve bucket docs
                bucket (dict, default=None): Bucket to load data from

            Returns:
                tuple (str, datetime, datetime): uid, now, receipt_by

        """

        if self.is_null() or transaction.value() <= 0:
            return None

        from Acquire.Accounting import Transaction as _Transaction

        if not isinstance(transaction, _Transaction):
            raise TypeError("The passed transaction must be a Transaction!")

        if authorisation_resource is None:
            authorisation_resource = transaction.fingerprint()
            accept_partial_match = True
        else:
            accept_partial_match = False

        self.assert_valid_authorisation(
                                    authorisation=authorisation,
                                    resource=authorisation_resource,
                                    accept_partial_match=accept_partial_match)

        bucket = self._get_account_bucket()

        balance = self.balance(bucket=bucket)

        if balance.available(self.get_overdraft_limit()) < transaction.value():
            from Acquire.Accounting import InsufficientFundsError
            raise InsufficientFundsError(
                "You cannot debit '%s' from account %s as there "
                "are insufficient funds in this account." %
                (transaction, str(self)))

        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import datetime_to_datetime \
            as _datetime_to_datetime
        from Acquire.ObjectStore import get_datetime_future \
            as _get_datetime_future
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Accounting import LineItem as _LineItem

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        from Acquire.Accounting import TransactionCode as _TransactionCode

        while True:
            # create a UID and datetime for this debit and record
            # it in the account
            now = self._get_safe_now()

            if is_provisional:
                if receipt_by is None:
                    receipt_by = _get_datetime_future(days=7)
                else:
                    receipt_by = _datetime_to_datetime(receipt_by)

                delta = (receipt_by - now).total_seconds()
                if delta < 3600:
                    from Acquire.Accounting import AccountError
                    raise AccountError(
                        "You cannot request a receipt to be provided less "
                        "than 1 hour into the future! %s versus %s is only "
                        "%s second(s) in the future!" %
                        (_datetime_to_string(receipt_by),
                         _datetime_to_string(now), delta))
            else:
                receipt_by = None

            # and to create a key to find this debit later. The key is made
            # up from the isoformat datetime of the debit and a random string
            from Acquire.ObjectStore import create_uuid as _create_uuid
            datetime_key = _datetime_to_string(now)
            uid = "%s/%s" % (datetime_key, _create_uuid()[0:8])

            # the key in the object store is a combination of the key for this
            # account plus the uid for the debit plus the actual debit value.
            # We record the debit value in the key so that we can accumulate
            # the balance from just the key names
            if is_provisional:
                encoded_value = _TransactionInfo.encode(
                                    _TransactionCode.CURRENT_LIABILITY,
                                    transaction.value())
            else:
                encoded_value = _TransactionInfo.encode(
                                    _TransactionCode.DEBIT,
                                    transaction.value())

            item_key = "%s/%s/%s" % (self._transactions_key(),
                                     uid, encoded_value)

            # create a line_item for this debit and save it to the object store
            line_item = _LineItem(uid, authorisation)

            # validate that we have not stepped into another hour...
            now2 = self._get_safe_now()

            if now.hour == now2.hour:
                # we are still in the same hour, so it is safe to
                # record the transaction
                break

        _ObjectStore.set_object_from_json(bucket=bucket, key=item_key,
                                          data=line_item.to_data())

        balance = self.balance(bucket=bucket)

        if balance.available(overdraft_limit=self._overdraft_limit) < 0:
            # This transaction has helped push the account beyond the
            # overdraft limit. This can only happen if two debits
            # take place at the same time - both should be refunded
            from Acquire.Accounting import TransactionInfo \
                as _TransactionInfo

            info = _TransactionInfo.from_key(item_key)
            info = _TransactionInfo.rescind(info)

            line_item = _LineItem(uid=info.dated_uid(), authorisation=None)

            item_key = "%s/%s" % (self._transactions_key(),
                                  info.to_key())

            _ObjectStore.set_object_from_json(bucket=bucket, key=item_key,
                                              data=line_item.to_data())

            raise InsufficientFundsError(
                "You cannot debit '%s' from account %s as there "
                "are insufficient funds in this account." %
                (transaction, str(self)))

        return (uid, now, receipt_by)

    def get_overdraft_limit(self):
        """Return the overdraft limit of this account

            Returns:
                Decimal: Overdraft limit
        """
        if self.is_null():
            return 0

        return self._overdraft_limit

    def set_group(self, group, bucket=None):
        """Set the Accounts group to which this account belongs"""
        if self.is_null():
            return

        from Acquire.Accounting import Accounts as _Accounts
        if not isinstance(group, _Accounts):
            raise TypeError("The Accounts group must be of type Accounts")

        if self._group_name != group.name():
            self._group_name = group.name()
            self._save_account(bucket=bucket)

    def set_overdraft_limit(self, limit, bucket=None):
        """Set the overdraft limit of this account to 'limit'

            Args:
                limit (int): Limit to set overdraft to
                TODO
                bucket (dict, default=None):

            Returns:
                None

        """
        if self.is_null():
            return

        from Acquire.Accounting import create_decimal as _create_decimal
        limit = _create_decimal(limit)
        if limit < 0:
            raise ValueError("You cannot set the overdraft limit to a "
                             "negative value! (%s)" % limit)

        old_limit = self._overdraft_limit

        if old_limit != limit:
            self._overdraft_limit = limit

            if self.is_beyond_overdraft_limit():
                # restore the old limit
                self._overdraft_limit = old_limit
                from Acquire.Accounting import AccountError
                raise AccountError("You cannot change the overdraft limit to "
                                   "%s as this is greater than the current "
                                   "balance!" % (limit))
            else:
                # save the new limit to the object store
                self._save_account(bucket)

    def is_beyond_overdraft_limit(self, bucket=None):
        """Return whether or not the current balance is beyond
           the overdraft limit

            Args:
                TODO
                bucket (dict, default=None):
            Returns:
                bool: True if over overdraft limit, else False
        """
        available = self.balance(bucket=bucket).available()
        return available < -(self.get_overdraft_limit())

    def _key(self):
        """Return the key for this account in the object store"""
        if self.is_null():
            return None
        else:
            return "%s/%s" % (_account_root(), self.uid())

    def _transactions_key(self):
        """Return the root key for the transactions for this account
           in the object store
        """
        if self.is_null():
            return None
        else:
            return "%s/txns" % self._key()

    def _balance_key(self):
        """Return the root key for the balances for this account
           in this object store
        """
        if self.is_null():
            return None
        else:
            return "%s/balance" % self._key()

    def _load_account(self, bucket=None):
        """Load the current state of the account from the object store"""
        if self.is_null():
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        bucket = self._get_account_bucket()

        try:
            data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                     key=self._key())
        except:
            data = None

        if data is None:
            from Acquire.Accounting import AccountError
            raise AccountError(
                "There is no account data for this account? %s" % self._key())

        import copy as _copy
        self.__dict__ = _copy.copy(Account.from_data(data).__dict__)

    def _save_account(self, bucket=None):
        """Save this account back to the object store"""
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        bucket = self._get_account_bucket()
        _ObjectStore.set_object_from_json(bucket=bucket,
                                          key=self._key(),
                                          data=self.to_data())

        # reload, in case anyone saved just after us...
        self._load_account(bucket=bucket)

    def to_data(self):
        """Return a dictionary that can be encoded to json from this object"""
        data = {}

        if not self.is_null():
            data["uid"] = self._uid
            data["name"] = self._name
            data["description"] = self._description
            data["overdraft_limit"] = str(self._overdraft_limit)
            data["aclrules"] = self._aclrules.to_data()
            data["group_name"] = self._group_name

        return data

    @staticmethod
    def from_data(data):
        """Construct and return an Account from the passed dictionary that has
           been decoded from json
        """
        account = Account()

        if (data and len(data) > 0):
            from Acquire.Accounting import create_decimal as _create_decimal
            from Acquire.Identity import ACLRules as _ACLRules

            account._uid = data["uid"]
            account._name = data["name"]
            account._description = data["description"]
            account._overdraft_limit = _create_decimal(data["overdraft_limit"])

            if "aclrules" in data:
                account._aclrules = _ACLRules.from_data(data["aclrules"])
            else:
                account._aclrules = _ACLRules.inherit()

            if "group_name" in data:
                account._group_name = data["group_name"]
            else:
                account._group_name = None

        return account
