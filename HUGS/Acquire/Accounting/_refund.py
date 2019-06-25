
__all__ = ["Refund"]


class Refund:
    """This class holds the refund for a transaction. This is sent
       by the credited account or other authorised party to refund a
       transaction. Refunds must succeed, so potentially they can
       allow accounts to breach overdraft limits etc.
    """
    def __init__(self, credit_note=None, authorisation=None):
        """Create a refund for the transaction that resulted in the passed
           credit note. Specify the authorisation of the refund. Note
           that transactions can only be refunded in full. If you want
           to give a partial refund then this is better handled as
           a new transaction from the credited to debited accounts.

           Note also that you can only refund completed transactions.
           If you need to refund a provisional transaction then receipt
           it first. (Obvs it is easier to just receipt with 0 rather
           than receipt and then refund...)
        """
        if credit_note is None:
            self._credit_note = None
            self._authorisation = None
            return

        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Identity import Authorisation as _Authorisation

        if not isinstance(credit_note, _CreditNote):
            raise TypeError("The credit note must be of type CreditNote")

        if not isinstance(authorisation, _Authorisation):
            raise TypeError("The authorisation must be of type Authorisation")

        self._credit_note = credit_note
        self._authorisation = authorisation

    def __str__(self):
        return "Refund(credit_note=%s)" % str(self.credit_note())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._credit_note == other._credit_note and \
                   self._authorisation == other._authorisation
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this Refund is null

           Returns:
                bool: True if refund is null, else False
        """
        return self._credit_note is None

    def credit_note(self):
        """Return the credit note that this is refunding

           Returns:
                CreditNote: CreditNote related to this Refund
        """
        if self.is_null():
            from Acquire.Accounting import CreditNote as _CreditNote
            return _CreditNote()
        else:
            return self._credit_note

    def transaction_uid(self):
        """Return the UID of the transaction for which this
           is the refund. The transaction UID is the same as the UID
           for the original debit note

           Returns:
                str: UID of transaction this is refunding
        """
        return self.debit_note_uid()

    def debit_note_uid(self):
        """Return the UID of the debit note that this is refunding

           Returns:
                str: UID of debit note this is refunding
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.debit_note_uid()

    def debit_account_uid(self):
        """Return the UID of the account from which this refund will
           return value

           Returns:
                str: UID of account to return value to
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.debit_account_uid()

    def credit_account_uid(self):
        """Return the UID of the account from which this refund will be
           drawn

           Returns:
                str: UID of account to draw refund from
        """
        if self.is_null():
            return None
        else:
            return self._credit_note.credit_account_uid()

    def transaction(self):
        """Return a transaction that corresponds to the real transfer
           of value back from the credit to debit accounts (remembering
           that the original debit account will be the new credit account,
           and the original credit account will be the new debit account).


           Returns:
                Transaction: Transaction related to this refund
        """
        from Acquire.Accounting import Transaction as _Transaction

        if self.is_null():
            return _Transaction()
        else:
            return _Transaction(self.value(),
                                "Refund for transaction %s"
                                % self.transaction_uid())

    def value(self):
        """Return the value of the refund

           Decimal: Value of this refund
        """
        if self.is_null():
            from Acquire.Accounting import create_decimal as _create_decimal
            return _create_decimal(0)
        else:
            return self._credit_note.value()

    def authorisation(self):
        """Return the authorisation for the refund

           Authorisation: Authorisation for this refund
        """
        return self._authorisation

    def to_data(self):
        """Return the data for this object as a dictionary that can be
           serialised to JSON

           Returns:
                dict: Dictionary to serialise to JSON

        """
        data = {}

        if not self.is_null():
            data["credit_note"] = self._credit_note.to_data()
            data["authorisation"] = self._authorisation.to_data()

        return data

    @staticmethod
    def from_data(data):
        """Return a Refund from the passed JSON-decoded dictionary

           Args:
                dict: JSON-decoded dictionary
           Returns:
                Refund: Refund object created from JSON


        """
        r = Refund()

        if (data and len(data) > 0):
            from Acquire.Accounting import CreditNote as _CreditNote
            from Acquire.Identity import Authorisation as _Authorisation
            r._credit_note = _CreditNote.from_data(data["credit_note"])
            r._authorisation = _Authorisation.from_data(data["authorisation"])

        return r

    @staticmethod
    def create(credit_notes, authorisation):
        """Construct a series of refunds from the passed credit notes,
           each of which is authorised using the passed authorisation.
           This will refund all of the transactions passed in full

           Args:
                credit_notes (list): Credit notes to process
                authorisation (Authorisatin): Authorisation for refunds

           Returns:
                Refund or list[Refunds]: Processed refunds for credit notes

        """
        try:
            credit_note = credit_notes[0]
        except:
            return Refund(credit_notes, authorisation)

        if len(credit_notes) == 0:
            return Refund()
        elif len(credit_notes) == 1:
            return Refund(credit_notes[0], authorisation)

        refunds = []

        for credit_note in credit_notes:
            refunds.append(Refund(credit_note, authorisation))

        return refunds
