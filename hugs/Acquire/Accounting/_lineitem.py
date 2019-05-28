
__all__ = ["LineItem"]


class LineItem:
    """This class holds the data for a line item in the account. This holds
       basic information about the item, e.g. its UID and authorisation
    """
    def __init__(self, uid=None, authorisation=None):
        self._uid = uid

        if authorisation is not None:
            from Acquire.Identity import Authorisation as _Authorisation

            if not isinstance(authorisation, _Authorisation):
                raise TypeError("Authorisation must be of type Authorisation!")

        self._authorisation = authorisation

    def __str__(self):
        return "LineItem(uid=%s)" % self._uid

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._uid == other._uid
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_null(self):
        """Return whether or not this is a null line item"""
        return self._uid is None

    def uid(self):
        """Return the UID of the TransactionRecord that provides
           more information about this line item in the ledger
        """
        return self._uid

    def authorisation(self):
        """Return the authorisation that was used to authorise this action"""
        return self._authorisation

    def to_data(self):
        """Return this object as a dictionary that can be serialised to json"""
        data = {}

        if not self.is_null():
            data["uid"] = self._uid

            if self._authorisation is not None:
                data["authorisation"] = self._authorisation.to_data()

        return data

    @staticmethod
    def from_data(data):
        """Return a LineItem constructed from the json-decoded dictionary"""
        l = LineItem()

        if (data and len(data) > 0):
            l._uid = data["uid"]

            if "authorisation" in data:
                from Acquire.Identity import Authorisation as _Authorisation
                l._authorisation = _Authorisation.from_data(
                    data["authorisation"])
            else:
                l._authorisation = None

        return l
