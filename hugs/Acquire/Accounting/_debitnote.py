
__all__ = ["DebitNote"]


class DebitNote:
    """This class holds all of the information about a completed debit. This
       is combined with credit note of equal value to form a transaction record
    """
    def __init__(self, transaction=None, account=None, authorisation=None,
                 is_provisional=False, receipt_by=None,
                 receipt=None, refund=None, authorisation_resource=None,
                 bucket=None):
        """Create a debit note for the passed transaction will debit value
           from the passed account. The note will create a unique ID (uid)
           for the debit, plus the datetime of the time that value was drawn
           from the debited account. This debit note will be paired with a
           corresponding credit note from the account that received the value
           from the transaction so that a balanced TransactionRecord can be
           written to the ledger. If the note is provisional, then the value
           of the transaction will be held until the corresponding CreditNote
           has been receipted. This must be receipted before 'receipt_by',
           else the value will be returned to the DebitNote account
           (it will be automatically refunded)
        """
        self._transaction = None

        nargs = (transaction is not None) + (refund is not None) + \
                (receipt is not None)

        if nargs > 1:
            raise ValueError("You can only choose to create a debit note "
                             "from a transaction, receipt or refund!")

        if refund is not None:
            self._create_from_refund(refund, account, bucket)
        elif receipt is not None:
            self._create_from_receipt(receipt, account, bucket)
        elif (transaction is not None):
            if account is None:
                raise ValueError("You need to supply the account from "
                                 "which the transaction will be taken")

            self._create_from_transaction(
                        transaction=transaction,
                        account=account,
                        authorisation=authorisation,
                        authorisation_resource=authorisation_resource,
                        is_provisional=is_provisional,
                        receipt_by=receipt_by, bucket=bucket)

    def __str__(self):
        if self.is_null():
            return "DebitNote::null"
        else:
            return "DebitNote:%s>>%s" % (self.account_uid(), self.value())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this is a null note

            Returns:
                bool: True if note not null, else False
        """
        return self._transaction is None

    def uid(self):
        """Return the UID for this note. This has the format
           dd:mm:yyyy/unique_string

           Returns:
                str: UID for this note
        """
        if self.is_null():
            return None
        else:
            return self._uid

    def datetime(self):
        """Return the datetime for when value was debited from the account

            Returns:
                datetime: When value was debited from account
        """
        if self.is_null():
            return None
        else:
            return self._datetime

    def account_uid(self):
        """Return the UID of the account that was debited

            Returns:
                str: UID of account that was debited
        """
        if self.is_null():
            return None
        else:
            return self._account_uid

    def transaction(self):
        """Return the transaction related to this debit note

            Returns:
                Transaction: Transaction related to this credit note
        """
        if self.is_null():
            return None
        else:
            return self._transaction

    def value(self):
        """Return the value of this note

            Returns:
                Decimal: Value of this note

        """
        if self.is_null():
            return 0
        else:
            return self.transaction().value()

    def authorisation(self):
        """Return the authorisation that was used successfully to withdraw
           value from the debited account

           Returns:
                Authorisation: Authorisation used to withdraw value from
                account
        """
        if self.is_null():
            return None
        else:
            return self._authorisation

    def is_provisional(self):
        """Return whether or not the debit was provisional. Provisional debits
           are listed as liabilities

           Returns:
                bool: True if debit was provisional, else False
        """
        if self.is_null():
            return False
        else:
            return self._is_provisional

    def needs_receipting(self):
        """Return whether or not this DebitNote transaction needs
           receipting - if it does, then it must be receipted
           by the CreditNote before DebitNote.receipt_by().

           Returns:
                bool: True if note needs receipting, else False
        """
        return self.is_provisional()

    def receipt_by(self):
        """Return the datetime by which this DebitNote must be
           receipted via the CreditNote, else the transaction
           will be automatically refunded. This will return
           'None' if the transaction has already been receipted
           or it wasn't provisional

           Returns:
                datetime: Date by which a receipt must be created
        """
        if self.is_provisional():
            return self._receipt_by
        else:
            return None

    def _create_from_refund(self, refund, account, bucket):
        """Function used to construct a debit note by extracting
           the value specified in the passed refund from the specified
           account. This is authorised using the authorisation held in
           the refund. Note that the refund must match
           up with a prior existing provisional transaction, and this
           must not have already been refunded. This will
           actually take value out of the passed account, with that
           value residing in this debit note until it is credited to
           another account

           Args:
                refund (Refund): Refund to create debit note from
                account (Account):
        """
        from Acquire.Accounting import Refund as _Refund

        if not isinstance(refund, _Refund):
            raise TypeError("You can only create a DebitNote with a "
                            "Refund")

        if refund.is_null():
            return

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import TransactionState as _TransactionState
        from Acquire.Accounting import Account as _Account

        # get the transaction behind this refund and move it into
        # the "refunding" state
        transaction = _TransactionRecord.load_test_and_set(
                        refund.transaction_uid(),
                        _TransactionState.DIRECT,
                        _TransactionState.REFUNDING, bucket=bucket)

        try:
            # ensure that the receipt matches the transaction...
            transaction.assert_matching_refund(refund)

            if account is None:
                account = _Account(transaction.credit_account_uid(), bucket)
            elif account.uid() != refund.credit_account_uid():
                raise ValueError("The accounts do not match when debiting "
                                 "the refund: %s versus %s" %
                                 (account.uid(), refund.credit_account_uid()))

            # now move the refund from the credit account back to the
            # debit note
            (uid, datetime) = account._debit_refund(refund, bucket)

            self._transaction = refund.transaction()
            self._account_uid = refund.credit_account_uid()
            self._authorisation = refund.authorisation()
            self._is_provisional = False

            self._datetime = datetime
            self._uid = str(uid)
        except:
            # move the transaction back to its original state...
            _TransactionRecord.load_test_and_set(
                        refund.transaction_uid(),
                        _TransactionState.REFUNDING,
                        _TransactionState.DIRECT)
            raise

    def _create_from_receipt(self, receipt, account, bucket):
        """Function used to construct a debit note by extracting
           the value specified in the passed receipt from the specified
           account. This is authorised using the authorisation held in
           the receipt, based on the original authorisation given in the
           provisional transaction. Note that the receipt must match
           up with a prior existing provisional transaction, and this
           must not have already been receipted or refunded. This will
           actually take value out of the passed account, with that
           value residing in this debit note until it is credited to
           another account

           Args:
                receipt (Receipt): Receipt to create DebitNote from
                account (Account): Account to take value from
                bucket (dict): Bucket to read data from
        """
        from Acquire.Accounting import Receipt as _Receipt

        if not isinstance(receipt, _Receipt):
            raise TypeError("You can only create a DebitNote with a "
                            "Receipt")

        if receipt.is_null():
            return

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import TransactionState as _TransactionState
        from Acquire.Accounting import Account as _Account

        # get the transaction behind this receipt and move it into
        # the "receipting" state
        transaction = _TransactionRecord.load_test_and_set(
                        receipt.transaction_uid(),
                        _TransactionState.PROVISIONAL,
                        _TransactionState.RECEIPTING, bucket=bucket)

        try:
            # ensure that the receipt matches the transaction...
            transaction.assert_matching_receipt(receipt)

            if account is None:
                account = _Account(transaction.debit_account_uid(), bucket)
            elif account.uid() != receipt.debit_account_uid():
                raise ValueError("The accounts do not match when debiting "
                                 "the receipt: %s versus %s" %
                                 (account.uid(), receipt.debit_account_uid()))

            # now move value from liability to debit, and then into this
            # debit note
            (uid, datetime) = account._debit_receipt(receipt, bucket)

            self._transaction = receipt.transaction()
            self._account_uid = receipt.debit_account_uid()
            self._authorisation = receipt.authorisation()
            self._is_provisional = False

            self._datetime = datetime
            self._uid = str(uid)
        except:
            # move the transaction back to its original state...
            _TransactionRecord.load_test_and_set(
                        receipt.transaction_uid(),
                        _TransactionState.RECEIPTING,
                        _TransactionState.PROVISIONAL)
            raise

    def _create_from_transaction(self, transaction, account, authorisation,
                                 authorisation_resource,
                                 is_provisional, receipt_by, bucket):
        """Function used to construct a debit note by extracting the
           specified transaction value from the passed account. This
           is authorised using the passed authorisation, and can be
           a provisional debit if 'is_provisional' is true. This will
           actually take value out of the passed account, with that
           value residing in this debit note until it is credited
           to another account

           Args:
                transaction (Transaction): Transaction that holds the value
                to be used
                account (Account): Account to take value from
                authorisation (Authorisation): Authorises the removal
                of value from account
                is_provisional (bool): Whether the debit is provisional or not
                receipt_by (datetime): Datetime by which debit must be
                receipted
                bucket (dict): Bucket to read data from
        """
        from Acquire.Accounting import Transaction as _Transaction
        from Acquire.Accounting import Account as _Account

        if not isinstance(transaction, _Transaction):
            raise TypeError("You can only create a DebitNote with a "
                            "Transaction")

        if not isinstance(account, _Account):
            raise TypeError("You can only create a DebitNote with a valid "
                            "Account")

        if authorisation is not None:
            from Acquire.Identity import Authorisation as _Authorisation

            if not isinstance(authorisation, _Authorisation):
                raise TypeError("Authorisation must be of type Authorisation")

        self._transaction = transaction
        self._account_uid = account.uid()
        self._authorisation = authorisation
        self._is_provisional = is_provisional

        (uid, datetime, receipt_by) = account._debit(
                        transaction=transaction,
                        authorisation=authorisation,
                        authorisation_resource=authorisation_resource,
                        is_provisional=is_provisional,
                        receipt_by=receipt_by, bucket=bucket)

        from Acquire.ObjectStore import datetime_to_datetime \
            as _datetime_to_datetime
        self._datetime = _datetime_to_datetime(datetime)
        self._uid = str(uid)

        if is_provisional:
            assert(receipt_by is not None)
            self._receipt_by = receipt_by
        else:
            assert(receipt_by is None)

    def to_data(self):
        """Return this DebitNote as a dictionary that can be encoded as json

               Returns:
                    dict: Dictionary to be converted to JSON

        """
        data = {}

        if not self.is_null():
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            data["transaction"] = self._transaction.to_data()
            data["account_uid"] = self._account_uid
            data["authorisation"] = self._authorisation.to_data()
            data["is_provisional"] = self._is_provisional
            data["datetime"] = _datetime_to_string(self._datetime)
            data["uid"] = self._uid

            if self._is_provisional:
                data["receipt_by"] = _datetime_to_string(self._receipt_by)

        return data

    @staticmethod
    def from_data(data):
        """Return a DebitNote that has been extracted from the passed
           json-decoded dictionary

           Args:
                data (dict): Dictionary from which to create object
           Returns:
                DebitNote: Created from dictionary
        """
        d = DebitNote()

        if (data and len(data) > 0):
            from Acquire.Accounting import Transaction as _Transaction
            from Acquire.Identity import Authorisation as _Authorisation
            from Acquire.ObjectStore import string_to_datetime \
                as _string_to_datetime

            d._transaction = _Transaction.from_data(data["transaction"])
            d._account_uid = data["account_uid"]
            d._authorisation = _Authorisation.from_data(data["authorisation"])
            d._is_provisional = data["is_provisional"]
            d._datetime = _string_to_datetime(data["datetime"])
            d._uid = data["uid"]

            if d._is_provisional:
                d._receipt_by = _string_to_datetime(data["receipt_by"])

        return d
