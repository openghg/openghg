from enum import Enum

__all__ = ["DataTypes", "ObsTypes"]


class DataTypes(Enum):
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    THAMESBARRIER = "THAMESBARRIER"
    ICOS = "ICOS"
    CRANFIELD = "CRANFIELD"
    BEACO2N = "BEACO2N"
    FOOTPRINTS = "FOOTPRINTS"


class ObsTypes(Enum):
    SURFACE = "ObsSurface"
    SATELLITE = "ObsSatellite"
    FLASK = "ObsFlask"
