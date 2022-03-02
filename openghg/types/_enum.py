from enum import Enum


class SurfaceTypes(Enum):
    """For standardising surface measurements"""

    BTT = "BTT"
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    THAMESBARRIER = "TMB"
    CRANFIELD = "CRANFIELD"
    BEACO2N = "BEACO2N"
    NPL = "NPL"
    AQMESH = "AQMESH"
    GLASGOW_PICARRO = "GLASGOW_PICARRO"
    GLASGOW_LICOR = "GLASGOW_LICOR"


class DataTypes(Enum):
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    THAMESBARRIER = "TMB"
    CRANFIELD = "CRANFIELD"
    BEACO2N = "BEACO2N"
    Footprints = "Footprints"
    NPL = "NPL"
    BTT = "BTT"
    AQMESH = "AQMESH"
    GLASGOW_PICARRO = "GLASGOW_PICARRO"
    GLASGOW_LICOR = "GLASGOW_LICOR"


class ObsTypes(Enum):
    SURFACE = "ObsSurface"
    SATELLITE = "ObsSatellite"
    FLASK = "ObsFlask"
    MOBILE = "ObsMobile"
