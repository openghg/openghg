
__all__ = ["Receipt"]


class Receipt:
    """This class holds the receipt for a provisional transaction. This is sent
       by the credited account to receipt that the service has been performed,
       and thus payment that is held as liability should now be paid. OR it
       sends back the fact that the service was not performed, and so the
       refund should be issued
    """
    def __init__(self, credit_note=None, authorisation=None,
                 receipted_value=None):
        """Create a receipt for the transaction that resulted in the passed
           credit note. Specify the authorisation of the receipt, and
           optionally specify the actual receipted value (which may be
           less than the value in the passed credit note)
        """
        if credit_note is None:
            self._credit_note = None
            self._authorisation = None
            from Acquire.Accounting import create_decimal as _create_decimal
            self._receipted_value = _create_decimal(0)
            return

        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Identity import Authorisation as _Authorisation

        if not isinstance(credit_note, _CreditNote):
            raise TypeError("The credit note must be of type CreditNote")

        if not isinstance(authorisation, _Authorisation):
            raise TypeError("The authorisation must be of type Authorisation")

        if not credit_note.is_provisional():
            raise ValueError("You cannot receipt a transaction that was "
                             "not provisional! - %s" % str(credit_note))

        if receipted_value is not None:
            from Acquire.Accounting import create_decimal as _create_decimal
            receipted_value = _create_decimal(receipted_value)

            if receipted_value < 0:
                raise ValueError("You cannot receipt a value that is less "
                                 "than zero! %s" % receipted_value)
            elif receipted_value > credit_note.value():
                raise ValueError("You cannot receipt a value that is greater "
                                 "than the value of the original credit "
                                 "note - %s versus %s" % (receipted_value,
                                                          credit_note.value()))
        else:
            receipted_value = credit_note.value()

        self._credit_note = credit_note
        self._authorisation = authorisation
        self._receipted_value = receipted_value

    def __str__(self):
        return "Receipt(credit_note=%s, receipted_value=%s)" % \
            (str(self.credit_note()), self.receipted_value())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._credit_note == other._credit_note and \
                   self._receipted_value == other._receipted_value and \
                   self._authorisation == other._authorisation
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this Receipt is null

           Returns:
                bool: True if reecipt null, else False

        """
        return self._credit_note is None

    def credit_note(self):
        """Return the credit note that this is a receipt for

           Returns:
                CreditNote: credit note related to this receipt

        """
        if self.is_null():
            from Acquire.Accounting import CreditNote as _CreditNote
            return _CreditNote()
        else:
            return self._credit_note

    def transaction_uid(self):
        """Return the UID of the provisional transaction for which this
           is the receipt. The transaction UID is the same as the UID
           for the original debit note

           Returns:
                str: UID of transaction

        """
        return self.debit_note_uid()

    def debit_note_uid(self):
        """Return the UID of the debit note that this is a receipt for

           Returns:
                str: Debit note UID
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.debit_note_uid()

    def debit_account_uid(self):
        """Return the UID of the account from which this receipt
           will debit value

           Returns:
                str: UID of account to debit
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.debit_account_uid()

    def credit_account_uid(self):
        """Return the UID of the account to which this receipt will
           credit value

           Returns:
                str: UID of account to credit
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.credit_account_uid()

    def transaction(self):
        """Return a transaction that corresponds to the real transfer
           of value between the debit and credit accounts. The value
           of the transaction is the actual receipted value

           Returns:
                Transaction: Transaction corresponding to this receipt
        """
        from Acquire.Accounting import Transaction as _Transaction
        if self.is_null():
            return _Transaction()
        else:
            return _Transaction(self.receipted_value(),
                                "Receipt for transaction %s"
                                % self.transaction_uid())

    def value(self):
        """Return the original (provisional) value of the transaction

           Returns:
                Decimal: Value of this receipt
        """
        if self.is_null():
            from Acquire.Accounting import create_decimal as _create_decimal
            return _create_decimal(0)
        else:
            return self._credit_note.value()

    def provisional_value(self):
        """Return the original (provisional) value of the transaction

           Returns:
                Decimal: Value of this receipt

        """
        return self.value()

    def receipted_value(self):
        """Return the receipted value. This is guaranteed to be less than
           or equal to the provisional value in the attached CreditNote

           Returns:
                Decimal: Receipted value
        """
        if self.is_null():
            from Acquire.Accounting import create_decimal as _create_decimal
            return _create_decimal(0)
        else:
            return self._receipted_value

    def authorisation(self):
        """Return the authorisation for the receipt

           Returns:
                Authorisation: Authorisation for the receipt
        """
        return self._authorisation

    def to_data(self):
        """Return the data for this object as a dictionary that can be
           serialised to JSON

           Returns:
                dict: Dictionary created from this object
        """
        data = {}

        if not self.is_null():
            data["credit_note"] = self._credit_note.to_data()
            data["authorisation"] = self._authorisation.to_data()
            data["receipted_value"] = str(self._receipted_value)

        return data

    @staticmethod
    def from_data(data):
        """Return a Receipt from the passed JSON-decoded dictionary

           Args:
                data (dict): JSON dictionary to create object
           Returns:
                Receipt: Receipt created from JSON

        """
        r = Receipt()

        if (data and len(data) > 0):
            from Acquire.Accounting import CreditNote as _CreditNote
            from Acquire.Accounting import create_decimal as _create_decimal
            from Acquire.Identity import Authorisation as _Authorisation
            r._credit_note = _CreditNote.from_data(data["credit_note"])
            r._authorisation = _Authorisation.from_data(data["authorisation"])
            r._receipted_value = _create_decimal(data["receipted_value"])

        return r

    @staticmethod
    def create(credit_notes, authorisation, receipted_value=None):
        """Construct a series of receipts from the passed credit notes,
           each of which is authorised using the passed authorisation.
           If 'receipted_value' is specified, then the sum total of
           receipts will equal 'receipted_value'. This cannot be
           greater than the sum of the passed credit notes. If it
           is less then the value, then the difference is subtracted
           from the first receipts returned

           Args:
             credit_notes (list): List of credit notes to receipt
             autorisation (Authorisation): Authorisation for credit notes
             receipted_value (Decimal, default=None): Total value to receipt

            Returns:
                Receipt or list[Receipt]: If <= 1 credit note Receipt, else
                list of Receipts

        """
        try:
            credit_note = credit_notes[0]
        except:
            return Receipt(credit_notes, authorisation, receipted_value)

        if len(credit_notes) == 0:
            return Receipt()
        elif len(credit_notes) == 1:
            return Receipt(credit_notes[0], authorisation, receipted_value)

        from Acquire.Accounting import create_decimal as _create_decimal
        total_value = _create_decimal(0)

        from Acquire.Accounting import CreditNote as _CreditNote

        for credit_note in credit_notes:
            if not isinstance(credit_note, _CreditNote):
                raise TypeError("The credit note must be of type CreditNote")

            total_value += credit_note.value()

        if receipted_value is None:
            receipted_value = total_value
        else:
            receipted_value = _create_decimal(receipted_value)

        if receipted_value < 0:
            raise ValueError("You cannot receipt a value that is less "
                             "than zero! %s" % receipted_value)
        elif receipted_value > total_value:
            raise ValueError("You cannot receipt a value that is greater "
                             "than the value of the original credit "
                             "notes - %s versus %s" % (receipted_value,
                                                       total_value))

        receipts = []

        if receipted_value == total_value:
            for credit_note in credit_notes:
                receipts.append(Receipt(credit_note, authorisation))
        else:
            diff = total_value - receipted_value

            for credit_note in credit_notes:
                d = min(diff, credit_note.value())

                diff -= d
                r = credit_note.value() - d

                receipts.append(Receipt(credit_note, authorisation, r))

        return receipts
