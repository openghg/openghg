
import uuid as _uuid
import datetime as _datetime
from copy import copy as _copy
from enum import Enum as _Enum

from ._errors import TransactionError, UnbalancedLedgerError, \
                     UnmatchedReceiptError, UnmatchedRefundError, \
                     LedgerError

__all__ = ["TransactionRecord", "TransactionState"]


class TransactionState(_Enum):
    """This class holds an enum of the current state of a transaction"""
    DIRECT = "DR"           # direct transaction, no receipts etc.
    PROVISIONAL = "PR"      # provisional transaction, needs receipt
    RECEIPTING = "RC"       # in process of being receipted...
    RECEIPTED = "RD"        # has been receipted
    REFUNDING = "RF"        # in process of being refunded...
    REFUNDED = "RR"         # has been refunded


class TransactionRecord:
    """This class holds a record of a transaction that has already been
       written to the accounting ledger. This records a unique ID, the
       datetime of the entry, the value, the two accounts involved in the
       transaction (debit account to credit account), a description of what
       the transaction refers to, and who/how the transaction was authorised.
       If 'is_provisional' then this is a provisional transaction that is
       recorded as a liability for the debtor and a future income for the
       creditor. This is confirmed by creating a receipt via "receipt_for"
       by passing the UID for the transaction this receipts, and the actual
       'value' of the receipt. Note that the actual value CANNOT exceed the
       original provisional value that was agreed by the debtor
    """
    def __init__(self, uid=None, bucket=None):
        """Load the transaction record from the object store using the
           passed UID

           Args:
                uid (str, default=None): UID for transaction record
                bucket (dict, default=None): Bucket to load data from
        """
        if uid:
            self._load_transaction(uid, bucket)
        else:
            self._debit_note = None
            self._credit_note = None
            self._transaction_state = None
            self._refund = None
            self._receipt = None

    def __str__(self):
        """Return a string representation of this transaction"""
        if self.is_null():
            return "TransactionRecord::null"

        return "[%s]: transferred %s from %s to %s | %s" % \
               (self.description(),
                self.value(), self.debit_note().account_uid(),
                self.credit_note().account_uid(),
                self._transaction_state.value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._debit_note == other._debit_note and \
                   self._credit_note == other._credit_note and \
                   self._transaction_state == other._transaction_state
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def reload(self):
        """Reload this transaction record from the object store. This is
           necessary if, e.g., the state of the record has been updated

           Returns:
                None
        """
        self._load_transaction(self.uid())

    def is_null(self):
        """Return whether or not this is a null record

           Returns:
            bool: True if this record is null, else False
        """
        return self._debit_note is None

    def description(self):
        """Return the description of this transaction

           Returns:
                str: Description of this transaction
        """
        if self.is_null():
            return None
        else:
            return self.transaction().description()

    def value(self):
        """Return the value of this transaction

           Returns:
                Decimal: Value of this transaction
        """
        if self.is_null():
            return 0
        else:
            return self.transaction().value()

    def uid(self):
        """Return the UID for this transaction record

           Returns:
                str: UID for this transaction record
        """
        if self.is_null():
            return None
        else:
            return self.debit_note().uid()

    def transaction(self):
        """Return the transaction underlying this record

           Returns:
                Transaction: Transaction underlying this TransactionRecord

        """
        if self.is_null():
            return None
        else:
            return self.debit_note().transaction()

    def transaction_state(self):
        """Return the current state of this transaction

           Returns:
                Enum: State of transaction

        """
        return self._transaction_state

    def assert_matching_refund(self, refund):
        """Assert that the passed refund matches this transaction

           Args:
                refund (Refund): Refund to check
        """
        if self.is_null():
            if not refund.is_null():
                raise UnmatchedRefundError(
                        "%s does not match a null TransactionRecord!" %
                        str(refund))

        match = True
        errors = []

        if refund.transaction_uid() != self.uid():
            match = False
            errors.append("UID: %s != %s" % (refund.transaction_uid(),
                                             self.uid()))
        elif refund.debit_account_uid() != self.debit_account_uid():
            match = False
            errors.append("DEBIT: %s != %s" % (refund.debit_account_uid(),
                                               self.debit_account_uid()))
        elif refund.credit_account_uid() != self.credit_account_uid():
            match = False
            errors.append("CREDIT: %s != %s" % (refund.credit_account_uid(),
                                                self.credit_account_uid()))
        elif refund.value() != self.value():
            match = False
            errors.append("VALUE: %s != %s" % (refund.value(), self.value()))

        if not match:
            raise UnmatchedRefundError(
                "The refund '%s' does not match the transaction '%s': %s" %
                (str(refund), str(self), " | ".join(errors)))

    def assert_matching_receipt(self, receipt):
        """Assert that the passed receipt matches this transaction"""
        if self.is_null():
            if not receipt.is_null():
                raise UnmatchedReceiptError(
                        "%s does not match a null TransactionRecord!" %
                        str(receipt))

        match = True
        errors = []

        if receipt.transaction_uid() != self.uid():
            match = False
            errors.append("UID: %s != %s" % (receipt.transaction_uid(),
                                             self.uid()))
        elif receipt.debit_account_uid() != self.debit_account_uid():
            match = False
            errors.append("DEBIT: %s != %s" % (receipt.debit_account_uid(),
                                               self.debit_account_uid()))
        elif receipt.credit_account_uid() != self.credit_account_uid():
            match = False
            errors.append("CREDIT: %s != %s" % (receipt.credit_account_uid(),
                                                self.credit_account_uid()))
        elif receipt.value() != self.value():
            match = False
            errors.append("VALUE: %s != %s" % (receipt.value(), self.value()))

        if not match:
            raise UnmatchedReceiptError(
                "The receipt '%s' does not match the transaction '%s': %s" %
                (str(receipt), str(self), " | ".join(errors)))

    def credit_account_uid(self):
        """Return the UID of the account to which value has been credited

           Returns:
                str: Account UID to which value has been credited
        """
        if self.is_null():
            return None
        else:
            return self.credit_note().account_uid()

    def credit_note(self):
        """Return the credit note for this transaction. This is the note
           recording that value has been credited to an account. A
           TransactionRecord is the pairing of a DebitNote with a CreditNote

           Returns:
                CreditNote: CreditNote for this transaction
        """
        return self._credit_note

    def debit_note(self):
        """Return the debit note for this transaction. This is the note
           recording that value has been debited to an account. A
           TransactionRecord is the pairing of a DebitNote with a CreditNote

           Returns:
                DebitNote: DebitNote for this transaction
        """
        return self._debit_note

    def debit_account_uid(self):
        """Return the UID of the account from which value has been debited

           Returns:
                str: Account UID from which value debited
        """
        if self.is_null():
            return None
        else:
            return self.debit_note().account_uid()

    def datetime(self):
        """Return the datetime when this transaction was applied

           Returns:
                datetime: Datetime at which transaction was applied
        """
        if self.is_null():
            return None
        else:
            return self.debit_note().datetime()

    def is_direct(self):
        """Return whether or not this transaction was direct (so was not
           provisional and so didn't need a receipt)

           Returns:
                bool: True if direct transaction, else False
        """
        return self._transaction_state == TransactionState.DIRECT

    def is_receipted(self):
        """Return whether or not this transaction has been receipted

           Returns:
                bool: True if transaction receipted, else False

        """
        return self._transaction_state == TransactionState.RECEIPTED

    def is_refunded(self):
        """Return whether or not this transaction has been refunded

           Returns:
                bool: True if transaction refunded, else False
        """
        return self._transaction_state == TransactionState.REFUNDED

    def is_provisional(self):
        """Return whether or not this transaction is provisional

           Returns:
                bool: True if transaction is provisional, else False
        """
        return self._transaction_state == TransactionState.PROVISIONAL

    def is_refund(self):
        """Return whether or not this transaction is a refund

           Returns:
                bool: True if transaction is a refund, else False

        """
        return self._refund is not None

    def is_receipt(self):
        """Return whether or not this transaction is a receipt

           Returns:
                bool: True if transaction is a receipt, else False

        """
        return self._receipt is not None

    def get_refund_info(self):
        """Return the reason for the refund

           Returns:
                Refund: Refund for this transaction
        """
        return self._refund

    def get_receipt_info(self):
        """Return the receipt underlying this transaction

           Returns:
                Receipt: Receipt for this transaction

        """
        return self._receipt

    def original_transaction_record(self):
        """If this is a receipt or refund transaction then return the
           original transaction record that this is receipting or refunding.
           Otherwise returns a null TransactionRecord

           Returns:
                TransactionRecord

        """
        if self.is_receipt():
            return TransactionRecord(self.get_receipt_info().transaction_uid())
        elif self.is_refund():
            return TransactionRecord(self.get_refund_info().transaction_uid())
        else:
            return TransactionRecord()

    def original_transaction(self):
        """If this is a receipt or refund transaction then return the
           original transaction that this is receipting or refunding.
           Otherwise returns a null Transaction

           Returns:
                TransactionRecord
        """
        if self.is_receipt() or self.is_refund():
            return self.original_transaction_record().transaction()
        else:
            from Acquire.Accounting import Transaction as _Transaction
            return _Transaction()

    def _load_transaction(self, uid, bucket=None):
        """Load this transaction from the object store

           Args:
                uid (str): UID of transaction to load
                bucket (dict): Bucket to load data from
           Returns:
                None
        """
        from Acquire.Accounting import Ledger as _Ledger
        self.__dict__ = _copy(_Ledger.load_transaction(
                                        uid, bucket=bucket).__dict__)

    def _save_transaction(self, bucket=None):
        """Save this transaction to the object store

           Args:
                bucket (dict): Bucket to load data from
           Returns:
                None
        """
        from Acquire.Accounting import Ledger as _Ledger
        _Ledger.save_transaction(self, bucket=bucket)

    @staticmethod
    def load_test_and_set(uid, expected_state, new_state,
                          bucket=None):
        """Static method to load up the Transaction record associated with
           the passed UID, check that the transaction state matches
           'expected_state', and if it does, to update the transaction
           state to 'new_state'. This returns the loaded (and updated)
           transaction

           Args:
                expected_state (TransactionState): State of transaction
                new_state (TransactionState): State to update transaction to
                bucket (dict): Bucket to load data from

           Returns:
                Transaction: Updated transaction
        """
        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.Accounting import Ledger as _Ledger
        from Acquire.ObjectStore import Mutex as _Mutex

        try:
            mutex = _Mutex(uid, timeout=600, lease_time=600)
        except Exception as e:
            raise LedgerError("Cannot secure a Ledger mutex for transaction "
                              "'%s'. Error = %s" % (uid, str(e)))

        try:
            transaction = _Ledger.load_transaction(uid, bucket)

            if transaction.transaction_state() != expected_state:
                raise TransactionError(
                    "Cannot update the state of the transaction %s from "
                    "%s to %s as it is not in the expected state" %
                    (str(transaction), expected_state.value, new_state.value))

            transaction._transaction_state = new_state
        except:
            mutex.unlock()
            raise

        # now need to write anything back if the state isn't changed
        if expected_state == new_state:
            return transaction

        # make sure we have enough time remaining on the lease to be
        # able to write this result back to the object store...
        if mutex.seconds_remaining_on_lease() < 100:
            try:
                mutex.fully_unlock()
            except:
                pass

            return TransactionRecord.load_test_and_set(uid, expected_state,
                                                       new_state, bucket)

        try:
            _Ledger.save_transaction(transaction, bucket)
        except:
            mutex.unlock()
            raise

        return transaction

    @staticmethod
    def from_data(data):
        """Construct and return a new Transaction from the passed JSON-decoded
            dictionary

            Args:
                data (dict): Dictionary from JSON
            Returns:
                TransactionRecord: Created from JSON
        """
        record = TransactionRecord()

        if (data and len(data) > 0):
            from Acquire.Accounting import CreditNote as _CreditNote
            from Acquire.Accounting import DebitNote as _DebitNote

            record._credit_note = _CreditNote.from_data(data["credit_note"])
            record._debit_note = _DebitNote.from_data(data["debit_note"])
            record._transaction_state = TransactionState(
                                            data["transaction_state"])

            if "refund" in data:
                from Acquire.Accounting import Refund as _Refund
                record._refund = _Refund.from_data(data["refund"])
            else:
                record._refund = None

            if "receipt" in data:
                from Acquire.Accounting import Receipt as _Receipt
                record._receipt = _Receipt.from_data(data["receipt"])
            else:
                record._receipt = None

        return record

    def to_data(self):
        """Return this transaction as a dictionary that can be
           encoded to JSON

           Returns:
                dict: Dictionary serialisable to JSON
        """
        data = {}

        if not self.is_null():
            data["credit_note"] = self._credit_note.to_data()
            data["debit_note"] = self._debit_note.to_data()
            data["transaction_state"] = self._transaction_state.value

            if self._refund is not None:
                data["refund"] = self._refund.to_data()

            if self._receipt is not None:
                data["receipt"] = self._receipt.to_data()

        return data
