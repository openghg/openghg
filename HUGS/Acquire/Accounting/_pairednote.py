
__all__ = ["PairedNote"]


class PairedNote:
    """This class holds a DebitNote together with its matching
       CreditNote(s)
    """
    def __init__(self, debit_note, credit_note):
        """Construct from the matching pair of notes"""
        from Acquire.Accounting import CreditNote as _CreditNote
        from Acquire.Accounting import DebitNote as _DebitNote

        if not isinstance(debit_note, _DebitNote):
            raise TypeError("The debit_note must be of type DebitNote!")

        if not isinstance(credit_note, _CreditNote):
            raise TypeError("The credit_note must be of type CreditNote!")

        if credit_note.debit_note_uid() != debit_note.uid():
            raise ValueError("You must pair up DebitNote (%s) with a "
                             "matching CreditNote (%s)" %
                             (debit_note.uid(), credit_note.debit_note_uid()))

        self._debit_note = debit_note
        self._credit_note = credit_note

    def __str__(self):
        return "PairedNote(debit_note=%s, credit_note=%s)" % \
                    (str(self._debit_note), str(self._credit_note))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._debit_note == other._debit_note and \
                   self._credit_note == other._credit_note
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def debit_note(self):
        """Return the debit note"""
        return self._debit_note

    def credit_note(self):
        """Return the credit note"""
        return self._credit_note

    @staticmethod
    def create(debit_notes, credit_notes):
        """Return a list of PairedNotes that pair together the passed
           debit notes and credit notes
        """
        try:
            debit_note = debit_notes[0]
        except:
            debit_notes = [debit_notes]

        if not isinstance(credit_notes, dict):
            try:
                credit_notes[0]
            except:
                credit_notes = [credit_notes]

            d = {}
            for credit_note in credit_notes:
                d[credit_note.debit_note_uid()] = credit_note
            credit_notes = d

        pairs = []
        missing = []

        for debit_note in debit_notes:
            if debit_note.uid() in credit_notes:
                pairs.append(PairedNote(debit_note,
                             credit_notes[debit_note.uid()]))
            else:
                missing.append(debit_note)

        if len(missing) > 0 or len(credit_notes) != len(debit_notes):
            from Acquire.Accounting import UnbalancedLedgerError
            raise UnbalancedLedgerError(
                "Cannot balance the ledger as the debit do not match the "
                "credits %s versus %s" % (str(debit_notes), str(credit_notes)))

        return pairs
