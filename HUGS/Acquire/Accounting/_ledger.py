
import uuid as _uuid
from copy import copy as _copy

__all__ = ["Ledger"]


class Ledger:
    """This is a static class which manages the global ledger for the
       entire accounting service
    """
    @staticmethod
    def get_key(uid):
        """Return the object store key for the transaction record with
           UID=uid

           Args:
                uid (str): UID to get key ofr
           Returns:
                str: Object store key for UID

        """
        return "accounting/transactions/%s" % (str(uid))

    @staticmethod
    def load_transaction(uid, bucket=None):
        """Load the transactionrecord with UID=uid from the ledger

           Args:
                uid (str): UID of transaction to load
                bucket (dict, default=None): Bucket to load data from
           Returns:
                TransactionRecord: Transaction with that UID

        """
        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        data = _ObjectStore.get_object_from_json(bucket, Ledger.get_key(uid))

        if data is None:
            from Acquire.Accounting import LedgerError
            raise LedgerError("There is no transaction recorded in the "
                              "ledger with UID=%s (at key %s)" %
                              (uid, Ledger.get_key(uid)))

        return _TransactionRecord.from_data(data)

    @staticmethod
    def save_transaction(record, bucket=None):
        """Save the passed transaction record to the object store

           Args:
                record (TransactionRecord): To save
                bucket (dict, default=None): Bucket to save data from
           Returns:
                None
        """
        from Acquire.Accounting import TransactionRecord as _TransactionRecord

        if not isinstance(record, _TransactionRecord):
            raise TypeError("You can only write TransactionRecord objects "
                            "to the ledger!")

        if not record.is_null():
            if bucket is None:
                from Acquire.Service import get_service_account_bucket \
                    as _get_service_account_bucket
                bucket = _get_service_account_bucket()

            from Acquire.ObjectStore import ObjectStore as _ObjectStore

            _ObjectStore.set_object_from_json(bucket,
                                              Ledger.get_key(record.uid()),
                                              record.to_data())

    @staticmethod
    def refund(refund, bucket=None):
        """Create and record a new transaction from the passed refund. This
           applies the refund, thereby transferring value from the credit
           account to the debit account of the corresponding transaction.
           Note that you can only refund a transaction once!
           This returns the (already recorded) TransactionRecord for the
           refund
        """
        from Acquire.Accounting import Refund as _Refund
        from Acquire.Accounting import Account as _Account
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Accounting import PairedNote as _PairedNote
        from Acquire.Accounting import TransactionRecord as _TransactionRecord

        if not isinstance(refund, _Refund):
            raise TypeError("The Refund must be of type Refund")

        if refund.is_null():
            return _TransactionRecord()

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        # return value from the credit to debit accounts
        debit_account = _Account(uid=refund.debit_account_uid(),
                                 bucket=bucket)
        credit_account = _Account(uid=refund.credit_account_uid(),
                                  bucket=bucket)

        # remember that a refund debits from the original credit account...
        # (and can only refund completed (DIRECT) transactions)
        debit_note = _DebitNote(refund=refund, account=credit_account,
                                bucket=bucket)

        # now create the credit note to return the value into the debit account
        try:
            credit_note = _CreditNote(debit_note=debit_note,
                                      refund=refund,
                                      account=debit_account,
                                      bucket=bucket)
        except Exception as e:
            # delete the debit note
            try:
                debit_account._delete_note(debit_note, bucket=bucket)
            except:
                pass

            # reset the transaction to its original state
            try:
                _TransactionRecord.load_test_and_set(
                        refund.transaction_uid(),
                        _TransactionState.REFUNDING,
                        _TransactionState.DIRECT,
                        bucket=bucket)
            except:
                pass

            raise e

        try:
            paired_notes = _PairedNote.create(debit_note, credit_note)
        except Exception as e:
            # delete all records...!
            try:
                debit_account._delete_note(debit_note, bucket=bucket)
            except:
                pass

            try:
                credit_account._delete_note(credit_note, bucket=bucket)
            except:
                pass

            # reset the transaction to the pending state
            try:
                _TransactionRecord.load_test_and_set(
                        refund.transaction_uid(),
                        _TransactionState.REFUNDING,
                        _TransactionState.DIRECT,
                        bucket=bucket)
            except:
                pass

            raise e

        # now record the two entries to the ledger. The below function
        # is guaranteed not to raise an exception
        return Ledger._record_to_ledger(paired_notes, refund=refund,
                                        bucket=bucket)

    @staticmethod
    def receipt(receipt, bucket=None):
        """Create and record a new transaction from the passed receipt. This
           applies the receipt, thereby actually transferring value from the
           debit account to the credit account of the corresponding
           transaction. Note that you can only receipt a transaction once!
           This returns the (already recorded) TransactionRecord for the
           receipt

           Args:
                receipt (Receipt): Receipt to use for transaction
                bucket (default=None): Bucket to load data from

           Returns:
                list: List of TransactionRecords

        """
        from Acquire.Accounting import Receipt as _Receipt
        from Acquire.Accounting import Account as _Account
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import PairedNote as _PairedNote

        if not isinstance(receipt, _Receipt):
            raise TypeError("The Receipt must be of type Receipt")

        if receipt.is_null():
            return _TransactionRecord()

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        # extract value into the debit note
        debit_account = _Account(uid=receipt.debit_account_uid(),
                                 bucket=bucket)
        credit_account = _Account(uid=receipt.credit_account_uid(),
                                  bucket=bucket)

        debit_note = _DebitNote(receipt=receipt, account=debit_account,
                                bucket=bucket)

        # now create the credit note to put the value into the credit account
        try:
            credit_note = _CreditNote(debit_note=debit_note,
                                      receipt=receipt,
                                      account=credit_account,
                                      bucket=bucket)
        except Exception as e:
            # delete the debit note
            try:
                debit_account._delete_note(debit_note, bucket=bucket)
            except:
                pass

            # reset the transaction to the pending state
            try:
                _TransactionRecord.load_test_and_set(
                        receipt.transaction_uid(),
                        _TransactionState.RECEIPTING,
                        _TransactionState.PENDING,
                        bucket=bucket)
            except:
                pass

            raise e

        try:
            paired_notes = _PairedNote.create(debit_note, credit_note)
        except Exception as e:
            # delete all records...!
            try:
                debit_account._delete_note(debit_note, bucket=bucket)
            except:
                pass

            try:
                credit_account._delete_note(credit_note, bucket=bucket)
            except:
                pass

            # reset the transaction to the pending state
            try:
                _TransactionRecord.load_test_and_set(
                        receipt.transaction_uid(),
                        _TransactionState.RECEIPTING,
                        _TransactionState.PENDING,
                        bucket=bucket)
            except:
                pass

            raise e

        # now record the two entries to the ledger. The below function
        # is guaranteed not to raise an exception
        return Ledger._record_to_ledger(paired_notes, receipt=receipt,
                                        bucket=bucket)

    @staticmethod
    def perform(transaction=None, transactions=None,
                debit_account=None, credit_account=None,
                authorisation=None,
                authorisation_resource=None,
                is_provisional=False, receipt_by=None, bucket=None):
        """Perform the passed transaction(s) between 'debit_account' and
           'credit_account', recording the 'authorisation' for this
           transaction. If 'is_provisional' then record this as a provisional
           transaction (liability for the debit_account, future unspendable
           income for the 'credit_account'). Payment won't actually be taken
           until the transaction is 'receipted' (which may be for less than
           (but not more than) then provisional value, and which must take
           place before 'receipt_by' (which will default to one week in
           the future if not supplied - the actual time is encoded
           in the returned TransactionRecords). Returns the (already
           recorded) TransactionRecord.

           Note that if several transactions are passed, then they must all
           succeed. If one of them fails then they are immediately refunded.

           Args:
                transactions (list) : List of Transactions to process
                debit_account (Account): Account to debit
                credit_account (Account): Account to credit
                authorisation (Authorisation): Authorisation for
                the transactions
                is_provisional (bool, default=False): Whether the transactions
                are provisional
                receipt_by (datetime, default=None): Date by which transactions
                must be receipted
                bucket (dict): Bucket to load data from

            Returns:
                list: List of TransactionRecords

        """
        from Acquire.Accounting import Account as _Account
        from Acquire.Identity import Authorisation as _Authorisation
        from Acquire.Accounting import DebitNote as _DebitNote
        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Accounting import Transaction as _Transaction
        from Acquire.Accounting import PairedNote as _PairedNote

        if not isinstance(debit_account, _Account):
            raise TypeError("The Debit Account must be of type Account")

        if not isinstance(credit_account, _Account):
            raise TypeError("The Credit Account must be of type Account")

        if not isinstance(authorisation, _Authorisation):
            raise TypeError("The Authorisation must be of type Authorisation")

        if is_provisional:
            is_provisional = True
        else:
            is_provisional = False

        if transactions is None:
            transactions = []
        elif isinstance(transactions, _Transaction):
            transactions = [transactions]

        if transaction is not None:
            transactions.insert(0, transaction)

        # remove any zero transactions, as they are not worth recording
        t = []
        for transaction in transactions:
            if not isinstance(transaction, _Transaction):
                raise TypeError("The Transaction must be of type Transaction")

            if transaction.value() >= 0:
                t.append(transaction)

        transactions = t

        if bucket is None:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            bucket = _get_service_account_bucket()

        # first, try to debit all of the transactions. If any fail (e.g.
        # because there is insufficient balance) then they are all
        # immediately refunded
        debit_notes = []
        try:
            for transaction in transactions:
                debit_notes.append(_DebitNote(
                    transaction=transaction,
                    account=debit_account,
                    authorisation=authorisation,
                    authorisation_resource=authorisation_resource,
                    is_provisional=is_provisional,
                    receipt_by=receipt_by, bucket=bucket))

                # ensure the receipt_by date for all notes is the same
                if is_provisional and (receipt_by is None):
                    receipt_by = debit_notes[0].receipt_by()

        except Exception as e:
            # refund all of the completed debits
            credit_notes = []
            debit_error = str(e)
            try:
                for debit_note in debit_notes:
                    debit_account._rescind_note(debit_note, bucket=bucket)
            except Exception as e:
                from Acquire.Accounting import UnbalancedLedgerError
                raise UnbalancedLedgerError(
                    "We have an unbalanced ledger as it was not "
                    "possible to refund a multi-part refused credit (%s): "
                    "Credit refusal error = %s. Refund error = %s" %
                    (str(debit_note), str(debit_error), str(e)))

            # raise the original error to show that, e.g. there was
            # insufficient balance
            raise e

        # now create the credit note(s) for this transaction. This will credit
        # the account, thereby transferring value from the debit_note(s) to
        # that account. If this fails then the debit_note(s) needs to
        # be refunded
        credit_notes = {}
        has_error = False
        credit_error = Exception()
        for debit_note in debit_notes:
            try:
                credit_note = _CreditNote(debit_note, credit_account,
                                          bucket=bucket)
                credit_notes[debit_note.uid()] = credit_note
            except Exception as e:
                has_error = True
                credit_error = e
                break

        if has_error:
            # something went wrong crediting the account... We need to refund
            # the transaction - first retract the credit notes...
            try:
                for credit_note in credit_notes.values():
                    credit_account._delete_note(credit_note, bucket=bucket)
            except Exception as e:
                from Acquire.Accounting import UnbalancedLedgerError
                raise UnbalancedLedgerError(
                    "We have an unbalanced ledger as it was not "
                    "possible to credit a multi-part debit (%s): Credit "
                    "refusal error = %s. Refund error = %s" %
                    (debit_notes, str(credit_error), str(e)))

            # now refund all of the debit notes
            try:
                for debit_note in debit_notes:
                    debit_account._delete_note(debit_note, bucket=bucket)
            except Exception as e:
                from Acquire.Accounting import UnbalancedLedgerError
                raise UnbalancedLedgerError(
                    "We have an unbalanced ledger as it was not "
                    "possible to credit a multi-part debit (%s): Credit "
                    "refusal error = %s. Refund error = %s" %
                    (debit_notes, str(credit_error), str(e)))

            raise credit_error

        try:
            paired_notes = _PairedNote.create(debit_notes, credit_notes)
        except Exception as e:
            # delete all of the notes...
            for debit_note in debit_notes:
                try:
                    debit_account._delete_note(debit_note, bucket=bucket)
                except:
                    pass

            for credit_note in credit_notes:
                try:
                    credit_account._delete_note(credit_note, bucket=bucket)
                except:
                    pass

            raise e

        # now write the paired entries to the ledger. The below function
        # is guaranteed not to raise an exception
        return Ledger._record_to_ledger(paired_notes, is_provisional,
                                        bucket=bucket)

    @staticmethod
    def _record_to_ledger(paired_notes, is_provisional=False,
                          receipt=None, refund=None, bucket=None):
        """Internal function used to generate and record transaction records
           from the passed paired debit- and credit-note(s). This will write
           the transaction record(s) to the object store, and will also return
           the record(s).

           Args:
                paired_notes (list): List of PairedNotes
                is_provisional (bool, default=False): Whether transactions
                are provisional or not
                receipt (Receipt, default=None): Receipt to use
                refund (Refund): Refund to use
                bucket (dict): Bucket to read data from

           Returns:
                TransactionRecord: Holds record of transactions

        """
        from Acquire.Accounting import Receipt as _Receipt
        from Acquire.Accounting import Refund as _Refund
        from Acquire.Accounting import TransactionRecord as _TransactionRecord
        from Acquire.Accounting import TransactionState as _TransactionState

        if receipt is not None:
            if not isinstance(receipt, _Receipt):
                raise TypeError("Receipts must be of type 'Receipt'")

        if refund is not None:
            if not isinstance(refund, _Refund):
                raise TypeError("Refunds must be of type 'Refund'")

        try:
            records = []

            if bucket is None:
                from Acquire.Service import get_service_account_bucket \
                    as _get_service_account_bucket
                bucket = _get_service_account_bucket()

            for paired_note in paired_notes:
                record = _TransactionRecord()
                record._debit_note = paired_note.debit_note()
                record._credit_note = paired_note.credit_note()

                if is_provisional:
                    record._transaction_state = _TransactionState.PROVISIONAL
                else:
                    record._transaction_state = _TransactionState.DIRECT

                if receipt is not None:
                    record._receipt = receipt

                if refund is not None:
                    record._refund = refund

                Ledger.save_transaction(record, bucket)

                records.append(record)

            return records

        except:
            # an error occurring here will break the system, which will
            # require manual cleaning. Mark this as broken!
            try:
                Ledger._set_truly_broken(paired_notes, bucket)
            except:
                pass

            raise SystemError("The ledger is in a very broken state!")

    @staticmethod
    def _set_truly_broken(paired_notes, bucket):
        """Internal function called when an irrecoverable error state
           is detected. This records the notes that caused the error and
           places the affected accounts into an error state
        """
        raise NotImplementedError("_set_truly_broken needs to be implemented!")
