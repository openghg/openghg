from enum import Enum as _Enum

__all__ = ["RootPaths", "DataTypes"]


class RootPaths(_Enum):
    DATA = "data"
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"


# Better name for this enum?
class DataTypes(_Enum):
    CRDS = "CRDS"
    GC = "GC"
    EUROCOM = "EUROCOM"
    NOAA = "NOAA"
    THAMESBARRIER = "THAMESBARRIER"
    CRANFIELD = "CRANFIELD"
