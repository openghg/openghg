from enum import Enum


class SurfaceTypes(Enum):
    """For standardising surface measurements"""

    BTT = "BTT"
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    GCWERKS_NC = "GCWERKS_NC"
    ICOS = "ICOS"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    THAMESBARRIER = "TMB"
    CRANFIELD = "CRANFIELD"
    BEACO2N = "BEACO2N"
    NPL = "NPL"
    AQMESH = "AQMESH"
    GLASGOW_PICARRO = "GLASGOW_PICARRO"
    GLASGOW_LICOR = "GLASGOW_LICOR"
    OPENGHG = "OPENGHG"


class ColumnTypes(Enum):
    """Types of column data files that can be standardised"""

    OPENGHG = "OPENGHG"


class ColumnSources(Enum):
    """Sources of column data that can be transformed"""

    GOSAT = "GOSAT"


class EmissionsTypes(Enum):
    """For standardising emissions/flux inputs"""

    OPENGHG = "OPENGHG"
    INTEM = "INTEM"


class EmissionsDatabases(Enum):
    """For extracting and transforming emissions/flux databases"""

    EDGAR = "EDGAR"


class ObsTypes(Enum):
    """ """

    SURFACE = "ObsSurface"
    COLUMN = "ObsColumn"
    MOBILE = "ObsMobile"


class DataTypes(Enum):
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    GCWERKS_NC = "GCWERKS_NC"
    NOAA = "NOAA"
    EUROCOM = "EUROCOM"
    ICOS = "ICOS"
    THAMESBARRIER = "TMB"
    CRANFIELD = "CRANFIELD"
    BEACO2N = "BEACO2N"
    Footprints = "Footprints"
    NPL = "NPL"
    BTT = "BTT"
    AQMESH = "AQMESH"
    GLASGOW_PICARRO = "GLASGOW_PICARRO"
    GLASGOW_LICOR = "GLASGOW_LICOR"
    OPENGHG = "OPENGHG"
    INTEM = "INTEM"
