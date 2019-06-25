
__all__ = ["Balance"]


class Balance:
    """Very simple class that holds the balance, liability and
       recievable values for an account at a point in time
    """
    def __init__(self, balance=None, liability=None, receivable=None,
                 _is_safe=False):
        """Construct, optionally specifying the starting balance,
           liability and receivable. These initialise to 0 if
           not set
        """
        if _is_safe:
            self._balance = balance
            self._liability = liability
            self._receivable = receivable
        else:
            from Acquire.ObjectStore import string_to_decimal \
                as _string_to_decimal

            self._balance = _string_to_decimal(balance, default=0)
            self._liability = _string_to_decimal(liability, default=0)
            self._receivable = _string_to_decimal(receivable, default=0)

    def balance(self):
        """Return the balance"""
        return self._balance

    def liability(self):
        """Return the liability"""
        return self._liability

    def receivable(self):
        """Return the receivable"""
        return self._receivable

    def available(self, overdraft_limit=None):
        """Return the available balance (balance - liability)"""
        if overdraft_limit is None:
            return self.balance() - self.liability()
        else:
            from Acquire.Accounting import create_decimal as _create_decimal
            overdraft_limit = _create_decimal(overdraft_limit)
            return self.balance() - self.liability() + overdraft_limit

    def is_overdrawn(self, overdraft_limit=None):
        """Return whether or not this balance is overdrawn"""
        return self.available(overdraft_limit=overdraft_limit) < 0

    def __str__(self):
        return "Balance(balance=%s, liability=%s, receivable=%s)" % \
                (self.balance(), self.liability(), self.receivable())

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        """Comparison"""
        if type(other) is Balance:
            return self._balance == other._balance and \
                   self._liability == other._liability and \
                   self._receivable == other._receivable
        else:
            return False

    def __add__(self, other):
        """Add balances together"""
        if type(other) is Balance:
            return Balance(balance=self._balance+other._balance,
                           liability=self._liability+other._liability,
                           receivable=self._receivable+other._receivable,
                           _is_safe=True)

        from Acquire.Accounting import TransactionInfo as _TransactionInfo
        if type(other) is _TransactionInfo:
            balance = self._balance
            liability = self._liability
            receivable = self._receivable

            if other.is_credit():
                balance += other.value()
            elif other.is_debit():
                balance -= other.value()
            elif other.is_liability():
                liability += other.value()
            elif other.is_accounts_receivable():
                receivable += other.value()
            elif other.is_received_receipt():
                balance -= other.receipted_value()
                liability -= other.original_value()
            elif other.is_sent_receipt():
                balance += other.receipted_value()
                receivable -= other.original_value()
            elif other.is_received_refund():
                balance += other.value()
            elif other.is_sent_refund():
                balance -= other.value()

            return Balance(balance=balance, liability=liability,
                           receivable=receivable, _is_safe=True)

        from Acquire.Accounting import Transaction as _Transaction
        if type(other) is _Transaction:
            return Balance(balance=self._balance+other.value(),
                           liability=self._liability,
                           receivable=self._receivable,
                           _is_safe=True)

        from Acquire.Accounting import create_decimal as _create_decimal
        value = _create_decimal(other)
        return Balance(balance=self._balance+value,
                       liability=self._liability,
                       receivable=self._receivable,
                       _is_safe=True)

    def __sub__(self, other):
        """Add balances together"""
        if type(other) is type(Balance):
            return Balance(balance=self._balance-other._balance,
                           liability=self._liability-other._liability,
                           receivable=self._receivable-other._receivable,
                           _is_safe=True)

        from Acquire.Accounting import Transaction as _Transaction
        if type(other) is _Transaction:
            return Balance(balance=self._balance-other.value(),
                           liability=self._liability,
                           receivable=self._receivable,
                           _is_safe=True)

        from Acquire.Accounting import create_decimal as _create_decimal
        value = _create_decimal(other)
        return Balance(balance=self._balance+value,
                       liability=self._liability,
                       receivable=self._receivable,
                       _is_safe=True)

    @staticmethod
    def total(balances):
        """Return the sum of the passed balances"""
        from Acquire.Accounting import create_decimal as _create_decimal
        balance = _create_decimal(0)
        liability = _create_decimal(0)
        receivable = _create_decimal(0)

        for b in balances:
            if type(b) is not Balance:
                raise TypeError("You can only sum Balance objects!")

            balance += b._balance
            liability += b._liability
            receivable += b._receivable

        return Balance(balance=balance, liability=liability,
                       receivable=receivable, _is_safe=True)

    def to_data(self):
        """Return this balance as a JSON-serialisable object"""
        data = {}

        from Acquire.ObjectStore import decimal_to_string \
            as _decimal_to_string
        data["balance"] = _decimal_to_string(self._balance)
        data["liability"] = _decimal_to_string(self._liability)
        data["receivable"] = _decimal_to_string(self._receivable)

        return data

    @staticmethod
    def from_data(data):
        """Construct a balance from the passed json-deserialised object"""
        if data is None or len(data) == 0:
            return Balance()

        from Acquire.ObjectStore import string_to_decimal \
            as _string_to_decimal

        balance = _string_to_decimal(data["balance"])
        liability = _string_to_decimal(data["liability"])
        receivable = _string_to_decimal(data["receivable"])

        return Balance(balance=balance, liability=liability,
                       receivable=receivable, _is_safe=True)
