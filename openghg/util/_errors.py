__all__ = ["InvalidSiteError", "UnknownDataError"]


class InvalidSiteError(Exception):
    """ Raised if an invalid site is passed """


class UnknownDataError(Exception):
    """ Raised if an unknown data type is passed """

