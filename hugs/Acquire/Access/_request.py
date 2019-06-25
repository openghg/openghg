
__all__ = ["Request"]


class Request:
    """This is the base class for all resource request classes. These
       classes are used to transmit information about a resource
       request, together with the user authorisation and account
       from which payment for the resource should be taken
    """
    def __init__(self):
        """Construct the resource request"""
        pass

    def is_null(self):
        """Return whether or not this request is null

        Returns:
            bool: True
        """

        return True

    def to_data(self):
        """Return this class as a json-serialisable dictionary

            Returns:
                dict: returns a JSON serialisable dictionary
                of this class
        """
        data = {}

        data["class"] = str(self.__class__.__name__)

        return data

    @staticmethod
    def from_data(data):
        """Construct a Request from the data in the JSON-deserialised
           dictionary

           Args:
                data (str): create a Request object from the JSON data
            Returns:
                Request: a Request object created from the JSON data
        """

        if (data and len(data) > 0):
            try:
                classname = data["class"]
            except:
                return Request()

            if classname == "FileWriteRequest":
                from ._filewriterequest import FileWriteRequest \
                                            as _FileWriteRequest
                return _FileWriteRequest.from_data(data)
            elif classname == "RunRequest":
                from ._runrequest import RunRequest as _RunRequest
                return _RunRequest.from_data(data)
            else:
                raise TypeError("Unknown type '%s'" % classname)

        return Request()

    def _from_data(self, data):
        """Call this function from derived classes to load data
           into this Request
        """
        pass
