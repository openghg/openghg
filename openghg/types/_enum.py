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


class FluxTypes(Enum):
    """For standardising flux/emissions inputs"""

    OPENGHG = "OPENGHG"
    INTEM = "INTEM"


class FluxDatabases(Enum):
    """For extracting and transforming flux/emissions databases"""

    EDGAR = "EDGAR"


class FootprintTypes(Enum):
    """For standardising footprint inputs"""

    # OPENGHG = "OPENGHG"  # To be implemented
    ACRG_ORG = "ACRG_ORG"


class ObsTypes(Enum):
    """ """

    SURFACE = "ObsSurface"
    COLUMN = "ObsColumn"
    MOBILE = "ObsMobile"


class OneD_types(Enum):
    """ For standardising one dimensional timeseries data"""

    CRF = "Crf"


class DataTypes(Enum):
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
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
