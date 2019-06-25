
__all__ = ["CreditNote"]


class CreditNote:
    """This class holds all of the information about a completed credit. This
       is combined with a debit note of equal value to form a transaction
       record
    """
    def __init__(self, debit_note=None, account=None,
                 receipt=None, refund=None, bucket=None):
        """Create the corresponding credit note for the passed debit_note. This
           will credit value from the note to the passed account. The credit
           will use the same UID as the debit, and the same datetime. This
           will then be paired with the debit note to form a TransactionRecord
           that can be written to the ledger
        """
        self._account_uid = None

        nargs = (receipt is not None) + (refund is not None)

        if nargs > 1:
            raise ValueError("You can create a CreditNote with a receipt "
                             "or a refund - not both!")

        if receipt is not None:
            self._create_from_receipt(debit_note, receipt, account, bucket)

        elif refund is not None:
            self._create_from_refund(debit_note, refund, account, bucket)

        elif (debit_note is not None) and (account is not None):
            self._create_from_debit_note(debit_note, account, bucket)

        else:
            self._debit_account_uid = None
            self._datetime = None
            self._uid = None
            self._debit_note_uid = None

            from Acquire.Accounting import create_decimal as _create_decimal
            self._value = _create_decimal(0)

    def __str__(self):
        if self.is_null():
            return "CreditNote::null"
        else:
            return "CreditNote:%s>>%s" % (self.value(), self.account_uid())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this note is null

            Returns:
                bool: True if note is null, else False
        """
        return self._uid is None

    def needs_receipting(self):
        """Return whether or not this CreditNote needs to be receipted.
           If so, then the funds are only held provisionally, and must
           be receipted by CreditNote.receipt_by() else they will
           be returned to the DebitNote account

           Returns:
                bool: True if CreditNote needs receipting, else False

        """
        return self.is_provisional()

    def receipt_by(self):
        """Return the datetime by which this credit note must be
           receipted, or else the funds will be returned. This will
           return None if the CreditNote does not need receipting

           Returns:
                datetime: Date by which the credit note must be receipted
        """
        if self.is_provisional():
            return self._receipt_by
        else:
            return None

    def account_uid(self):
        """Return the UID of the account to which the value was credited

            Returns:
                str: Account UID
        """
        if self.is_null():
            return None
        else:
            return self._account_uid

    def credit_account_uid(self):
        """Synonym for self.account_uid()

            Returns:
                str: Account UID
        """
        return self.account_uid()

    def debit_account_uid(self):
        """Return the UID of the account from which the value was debited

            Returns:
                str: UID of account from which value was debited
        """
        if self.is_null():
            return None
        else:
            return self._debit_account_uid

    def datetime(self):
        """Return the datetime for this credit note

            Returns:
                datetime: Datetime for this credit note
        """
        return self._datetime

    def uid(self):
        """Return the UID of this credit note. This will not match the debit
           note UID - you need to use debit_note_uid() to get the UID of
           the debit note that matches this credit note

           Returns:
                str: UID of credit note
        """
        return self._uid

    def debit_note_uid(self):
        """Return the UID of the debit note that matches this credit note.
           While at the moment only a single credit note matches a debit note,
           it may be in the future that we divide a credit over several
           accounts (and thus several credit notes)

           Returns:
                str: UID of the debit note to match this credit note

        """
        return self._debit_note_uid

    def value(self):
        """Return the value of this note. This may be less than the
           corresponding debit note if only part of the value of the
           debit note is transferred into the account

           Returns:
                Decimal: Value of this note
        """
        return self._value

    def fingerprint(self):
        """Return a fingerprint for this credit note"""
        return "%s|%s" % (self._debit_note_uid, self._uid)

    def is_provisional(self):
        """Return whether or not this credit note is provisional
           (i.e. the value will only be transferred on completion
           of work and provision of a receipt. Note that the value
           will only be transferred if this CreditNote is receipted
           before CreditNote.receipt_by())

           Returns:
                bool: True if note is provisional, else False
        """
        if self.is_null():
            return False
        else:
            return self._is_provisional

    def _create_from_refund(self, debit_note, refund, account, bucket):
        """Internal function used to create the credit note from
           the passed refund. This will actually transfer value from the
           debit note to the credited account (which was the original
           debited account)

           debit_note (DebitNote): DebitNote to use
           refund (Refund): Refund from which to take value to create
           CreditNote
           account (Account): Account to credit refund to
           bucket (Bucket): Bucket to load data from

           Returns:
                None
        """
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import Refund as _Refund
        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import TransactionState as _TransactionState
        from Acquire.Accounting import Account as _Account

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("You can only create a CreditNote "
                            "with a DebitNote")

        if not isinstance(refund, _Refund):
            raise TypeError("You can only refund a Refund object: %s"
                            % str(refund.__class__))

        # get the transaction behind this refund and ensure it is in the
        # refunding state...
        transaction = _TransactionRecord.load_test_and_set(
                        refund.transaction_uid(),
                        _TransactionState.REFUNDING,
                        _TransactionState.REFUNDING, bucket=bucket)

        # ensure that the receipt matches the transaction...
        transaction.assert_matching_refund(refund)

        if account is None:
            account = _Account(transaction.debit_account_uid(), bucket)
        elif account.uid() != refund.debit_account_uid():
            raise ValueError("The accounts do not match when refunding "
                             "the receipt: %s versus %s" %
                             (account.uid(), refund.debit_account_uid()))

        (uid, datetime) = account._credit_refund(debit_note, refund, bucket)

        self._account_uid = account.uid()
        self._debit_account_uid = debit_note.account_uid()
        self._datetime = datetime
        self._uid = uid
        self._debit_note_uid = debit_note.uid()
        self._value = debit_note.value()
        self._is_provisional = debit_note.is_provisional()

        if self._is_provisional:
            self._receipt_by = debit_note.receipt_by()

        # finally(!) move the transaction into the refunded state
        _TransactionRecord.load_test_and_set(
                            refund.transaction_uid(),
                            _TransactionState.REFUNDING,
                            _TransactionState.REFUNDED, bucket=bucket)

    def _create_from_receipt(self, debit_note, receipt, account, bucket):
        """Internal function used to create the credit note from
           the passed receipt. This will actually transfer value from the
           debit note to the credited account

           debit_note (DebitNote): DebitNote from which to take value
           receipt (Receipt): Receipt to create CreditNote from
           account (Account): Account to credit
           bucket (Bucket): Bucket to load data from

           Returns:
                None

        """
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import Refund as _Refund
        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import TransactionState as _TransactionState
        from Acquire.Accounting import Account as _Account
        from Acquire.Accounting import Receipt as _Receipt

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("You can only create a CreditNote "
                            "with a DebitNote")

        if not isinstance(receipt, _Receipt):
            raise TypeError("You can only receipt a Receipt object: %s"
                            % str(receipt.__class__))

        # get the transaction behind this receipt and ensure it is in the
        # receipting state...
        transaction = _TransactionRecord.load_test_and_set(
                        receipt.transaction_uid(),
                        _TransactionState.RECEIPTING,
                        _TransactionState.RECEIPTING, bucket=bucket)

        # ensure that the receipt matches the transaction...
        transaction.assert_matching_receipt(receipt)

        if account is None:
            account = _Account(transaction.credit_account_uid(), bucket)
        elif account.uid() != receipt.credit_account_uid():
            raise ValueError("The accounts do not match when crediting "
                             "the receipt: %s versus %s" %
                             (account.uid(), receipt.credit_account_uid()))

        (uid, datetime) = account._credit_receipt(debit_note, receipt, bucket)

        self._account_uid = account.uid()
        self._debit_account_uid = debit_note.account_uid()
        self._datetime = datetime
        self._uid = uid
        self._debit_note_uid = debit_note.uid()
        self._value = debit_note.value()
        self._is_provisional = debit_note.is_provisional()

        if debit_note.is_provisional():
            self._receipt_by = debit_note.receipt_by()

        # finally(!) move the transaction into the receipted state
        _TransactionRecord.load_test_and_set(
                            receipt.transaction_uid(),
                            _TransactionState.RECEIPTING,
                            _TransactionState.RECEIPTED, bucket=bucket)

    def _create_from_debit_note(self, debit_note, account, bucket):
        """Internal function used to create the credit note that matches
           the passed debit note. This will actually transfer value from
           the debit note to the passed account

           debit_note (DebitNote): DebitNote to take value from
           account (Account): Account to credit
           bucket (Bucket): Bucket to load data from

           Returns:
                None
        """
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import Account as _Account

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("You can only create a CreditNote "
                            "with a DebitNote")

        if not isinstance(account, _Account):
            raise TypeError("You can only create a CreditNote with an "
                            "Account")

        (uid, datetime) = account._credit(debit_note, bucket=bucket)

        self._account_uid = account.uid()
        self._debit_account_uid = debit_note.account_uid()
        self._datetime = datetime
        self._uid = uid
        self._debit_note_uid = debit_note.uid()
        self._value = debit_note.value()
        self._is_provisional = debit_note.is_provisional()

        if self._is_provisional:
            self._receipt_by = debit_note.receipt_by()

    @staticmethod
    def from_data(data):
        """Construct and return a new CreditNote from the passed json-decoded
            dictionary

            Args:
                data (dict): JSON serialised dictionary of object
            Returns:
                CreditNote: CreditNote created from JSON data
        """
        note = CreditNote()

        if (data and len(data) > 0):
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime
            from Acquire.Accounting import create_decimal as _create_decimal

            note._account_uid = data["account_uid"]
            note._debit_account_uid = data["debit_account_uid"]
            note._uid = data["uid"]
            note._debit_note_uid = data["debit_note_uid"]
            note._datetime = _string_to_datetime(data["datetime"])
            note._value = _create_decimal(data["value"])
            note._is_provisional = data["is_provisional"]

            if note._is_provisional:
                note._receipt_by = _string_to_datetime(data["receipt_by"])

        return note

    def to_data(self):
        """Return this credit note as a dictionary that can be
           encoded to JSON

           Returns:
                dict: Dictionary of object to be encoded to JSON
        """
        data = {}

        if not self.is_null():
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string

            data["account_uid"] = self._account_uid
            data["debit_account_uid"] = self._debit_account_uid
            data["uid"] = self._uid
            data["debit_note_uid"] = self._debit_note_uid
            data["datetime"] = _datetime_to_string(self._datetime)
            data["value"] = str(self._value)
            data["is_provisional"] = self._is_provisional

            if self._is_provisional:
                data["receipt_by"] = _datetime_to_string(self._receipt_by)

        return data
