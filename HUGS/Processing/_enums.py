from enum import Enum

__all__ = ["RootPaths", "DataTypes"]


class RootPaths(Enum):
    DATA = "data"
    DATASOURCE = "datasource"
    INSTRUMENT = "instrument"
    SITE = "site"
    NETWORK = "network"


# Better name for this enum?
class DataTypes(Enum):
    CRDS = "CRDS"
    GC = "GC"
    EUROCOM = "EUROCOM"
    NOAA = "NOAA"
    THAMESBARRIER = "THAMESBARRIER"
    CRANFIELD = "CRANFIELD"
