
from enum import Enum as _Enum

__all__ = ["TransactionInfo", "TransactionCode"]


class TransactionCode(_Enum):
    CREDIT = "CR"
    DEBIT = "DR"
    CURRENT_LIABILITY = "CL"
    ACCOUNT_RECEIVABLE = "AR"
    RECEIVED_RECEIPT = "RR"
    SENT_RECEIPT = "SR"
    RECEIVED_REFUND = "RF"
    SENT_REFUND = "SF"


class TransactionInfo:
    """This class is used to encode and extract the type of transaction
       and value to/from an object store key
    """
    def __init__(self, key=None):
        """Construct, optionally from the passed key"""
        if key is not None:
            t = TransactionInfo.from_key(key)

            import copy as _copy
            self.__dict__ = _copy.copy(t.__dict__)
        else:
            from Acquire.Accounting import create_decimal as _create_decimal
            self._value = _create_decimal(0)
            self._receipted_value = _create_decimal(0)
            self._code = None
            self._datetime = None
            self._uid = None

    def __str__(self):
        if self._receipted_value is None:
            return "TransactionInfo(code==%s, value==%s)" % \
                        (self._code.value, self._value)
        else:
            return "TransactionInfo(code==%s, value==%s, receipted==%s)" % \
                        (self._code.value, self._value, self._receipted_value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._code == other._code and \
                   self._value == other._value
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def _get_code(code):
        """Return the TransactionCode matching 'code'"""
        return TransactionCode(code)

    @staticmethod
    def encode(code, value, receipted_value=None):
        """Encode the passed code and value into a simple string that can
           be used as part of an object store key. If 'receipted_value' is
           passed, then encode the receipted value of the provisional
           transaction too
        """
        if receipted_value is None:
            return "%2s%013.6f" % (code.value, value)
        else:
            return "%2s%013.6fT%013.6f" % (code.value, value, receipted_value)

    def rescind(self):
        """Return a TransactionInfo that corresponds to rescinding this
           transaction info. This is useful if you want to update the
           ledger to remove this object (since we can't delete anything
           from the ledger)
        """
        t = TransactionInfo()
        t._uid = self._uid[-1::-1]
        t._value = self._value
        t._datetime = self._datetime

        if self._code is TransactionCode.DEBIT:
            t._code = TransactionCode.CREDIT
        elif self._code is TransactionCode.CREDIT:
            t._code = TransactionCode.DEBIT
        elif self._code is TransactionCode.CURRENT_LIABILITY:
            t._value = -(self._value)
        elif self._code is TransactionCode.ACCOUNT_RECEIVABLE:
            t._value = -(self._value)
        else:
            raise PermissionError(
                "Do not have permission to rescind a %s" % str(self))

        return t

    def value(self):
        """Return the value of the transaction. This will be the receipted
           value if this has been set"""
        if self._receipted_value is not None:
            return self._receipted_value
        else:
            return self._value

    def uid(self):
        """Return the UID of this transaction"""
        return self._uid

    def dated_uid(self):
        """Return the full dated uid of the transaction. This
           is isoformat(datetime)/uid
        """
        return "%s/%s" % (self._datetime.isoformat(), self._uid)

    def datetime(self):
        """Return the datetime of this transaction"""
        return self._datetime

    @staticmethod
    def from_key(key):
        """Extract information from the passed object store key.

           This looks for a string that is;

           isoformat_datetime/UID/transactioncode

           where transactioncode is a string that matches
           '2 letters followed by a number'

           CL000100.005000
           DR000004.234100

           etc.

           For sent and received receipts there are two values;
           the receipted value and the original estimate. These
           have the standard format if the values are the same, e.g.

           RR000100.005000

           however, they have original value T receipted value if they are
           different, e.g.

           RR000100.005000T000090.000000

           Args:
                key: Object store key

        """
        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime
        from Acquire.Accounting import create_decimal as _create_decimal

        parts = key.split("/")

        # start at the end...
        nparts = len(parts)
        for i in range(0, nparts):
            j = nparts - i - 1
            t = TransactionInfo()

            try:
                t._datetime = _string_to_datetime(parts[j-2])
            except:
                continue

            t._uid = parts[j-1]

            part = parts[j]
            try:
                code = TransactionInfo._get_code(part[0:2])

                if code == TransactionCode.SENT_RECEIPT or \
                   code == TransactionCode.RECEIVED_RECEIPT:
                    values = part[2:].split("T")
                    try:
                        value = _create_decimal(values[0])
                        receipted_value = _create_decimal(values[1])
                        t._code = code
                        t._value = value
                        t._receipted_value = receipted_value
                        return t
                    except:
                        pass

                value = _create_decimal(part[2:])

                t._code = code
                t._value = value
                t._receipted_value = None

                return t
            except:
                pass

        raise ValueError("Cannot extract transaction info from '%s'"
                         % (key))

    def to_key(self):
        """Return this transaction encoded to a key"""
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string

        return "%s/%s/%s" % (_datetime_to_string(self._datetime),
                             self._uid,
                             TransactionInfo.encode(
                                    code=self._code,
                                    value=self._value,
                                    receipted_value=self._receipted_value))

    def receipted_value(self):
        """Return the receipted value of the transaction. This may be
           different to value() when the transaction was provisional,
           and the receipted value is less than the provisional value.
           This returns None if this transaction wasn't receipted

           Returns:
                Decimal: Receipted value of Transaction
        """
        return self._receipted_value

    def original_value(self):
        """Return the original (pre-receipted) value of the transaction"""
        return self._value

    def is_credit(self):
        """Return whether or not this is a credit

           Returns:
                bool: True if this is a credit, else False
        """
        return self._code == TransactionCode.CREDIT

    def is_debit(self):
        """Return whether or not this is a debit

           Returns:
                bool: True if this is a debit, else False

        """
        return self._code == TransactionCode.DEBIT

    def is_liability(self):
        """Return whether or not this is a liability

           Returns:
                bool: True if this is a liability, else False

        """
        return self._code == TransactionCode.CURRENT_LIABILITY

    def is_accounts_receivable(self):
        """Return whether or not this is accounts receivable

           Returns:
                bool: True if this is accounts receivable, else False

        """
        return self._code == TransactionCode.ACCOUNT_RECEIVABLE

    def is_sent_receipt(self):
        """Return whether or not this is a sent receipt

           Returns:
                bool: True if this is accounts receivable, else False

        """
        return self._code == TransactionCode.SENT_RECEIPT

    def is_received_receipt(self):
        """Return whether or not this is a received receipt

           Returns:
                bool: True if this is a received receipt, else False
        """
        return self._code == TransactionCode.RECEIVED_RECEIPT

    def is_sent_refund(self):
        """Return whether or not this is a sent refund

           Returns:
                bool: True if this is a sent refund, else False
        """
        return self._code == TransactionCode.SENT_REFUND

    def is_received_refund(self):
        """Return whether or not this is a received refund

           Returns:
                bool: True if this is a received refund, else False
        """
        return self._code == TransactionCode.RECEIVED_REFUND
