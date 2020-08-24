from enum import Enum

__all__ = ["DataTypes"]


class DataTypes(Enum):
    CRDS = "CRDS"
    GC = "GC"
    FOOTPRINT = "FOOTPRINT"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    THAMESBARRIER = "THAMESBARRIER"
    ICOS = "ICOS"
    CRANFIELD = "CRANFIELD"
