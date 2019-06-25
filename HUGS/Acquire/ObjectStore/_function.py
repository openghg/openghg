
__all__ = ["Function"]


class Function:
    """This is the base class of functions that can be stored to
       the ObjectStore for later execution
    """
    def __init__(self, function=None, **kwargs):
        """Create a function from the passed function, optionally
           supplying arguments that should be bound to this function
           at call time
        """
        self._args = None
        self._func = None

        if function is not None:
            if not hasattr(function, "__call__"):
                raise TypeError("You can only create a function from "
                                "callable objects")

            self._func = function

            args = {}

            for (item, value) in kwargs.items():
                args[item] = value

            if len(args) > 0:
                self._args = args

    def __call__(self, **kwargs):
        """Call this function. Any bound arguments are added to
           'kwargs' before calling
        """
        if self._args is not None:
            for (key, value) in self._args.items():
                if key not in kwargs:
                    kwargs[key] = value

        if self._func is not None:
            return self._func(**kwargs)
        else:
            return None

    def to_data(self, args_to_data=None):
        """Return a JSON-serialisable dictionary describing this function

           Args:
                args_to_data (default=None): Arguments to pass
           Returns:
                dict: JSON serialisable dict version of this function
        """
        data = {}

        if self._func is not None:
            data["function"] = self._func.__name__
            data["module"] = self._func.__module__

            if self._args is not None:
                if args_to_data is None:
                    data["args"] = self._args
                else:
                    data["args"] = args_to_data(self._args)

        return data

    @staticmethod
    def from_data(data, args_from_data=None):
        """Return the function from the JSON-deserialised dictionary

           Args:
                data (dict): Dict to create function from
                args_to_data (default=None): Arguments to pass
           Returns:
                function: Function created from data
        """

        f = Function()

        if data is not None and len(data) > 0:
            import importlib as _importlib
            m = _importlib.import_module(data["module"])
            f._func = getattr(m, data["function"])

            if "args" in data:
                if args_from_data is None:
                    f._args = data["args"]
                else:
                    f._args = args_from_data(data["args"])

        return f
