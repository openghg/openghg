
__all__ = ["create_decimal", "get_decimal_context"]


def get_decimal_context():
    """Return the context used for all decimals in transactions. This
       context rounds to 6 decimal places and provides sufficient precision
       to support any value between 0 and 999,999,999,999,999.999,999,999
       (i.e. everything up to just under one quadrillion - I doubt we will
        ever have an account that has more than a trillion units in it!)

        Returns:
            Context: The context (specifying precision etc.) of the decimals
            used in transactions

    """

    from decimal import Context as _Context
    return _Context(prec=24)


def create_decimal(value, default=0):
    """Create a decimal from the passed value. This is a decimal that
       has 6 decimal places and is clamped between
       -1 quadrillion < value < 1 quadrillion

       Args:
            value: Value to convert to Decimal
        Returns:
            Decimal: Decimal version of value

    """

    from decimal import Decimal as _Decimal

    if value is None:
        return _Decimal(0, get_decimal_context())

    try:
        d = _Decimal("%.6f" % value, get_decimal_context())
    except:
        value = _Decimal(value, get_decimal_context())
        d = _Decimal("%.6f" % value, get_decimal_context())

    if d <= -1000000000000:
        from Acquire.Accounting import AccountError
        raise AccountError(
                "You cannot create a balance with a value less than "
                "-1 quadrillion! (%s)" % (value))

    elif d >= 1000000000000000:
        from Acquire.Accounting import AccountError
        raise AccountError(
                "You cannot create a balance with a value greater than "
                "1 quadrillion! (%s)" % (value))

    return d
