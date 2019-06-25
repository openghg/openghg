
__all__ = ["lazy_import"]


class lazy_import:
    """This is not lazy_import, but instead a thin stub that matches the
       API but DOES NOT lazy_import anything. This imports at call time.
       Use this module if you are running a python installation that
       does not have lazy_import installed, e.g. because you don't want
       to install any GPL modules
    """
    @staticmethod
    def lazy_module(m):
        return __import__(m, fromlist=[''])

    @staticmethod
    def lazy_function(f):
        module_name, unit_name = f.rsplit('.', 1)
        return getattr(__import__(module_name, fromlist=['']), unit_name)

    @staticmethod
    def lazy_class(c):
        return lazy_import.lazy_function(c)
