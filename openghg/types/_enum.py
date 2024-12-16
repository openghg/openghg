from enum import Enum


class SurfaceTypes(Enum):
    """For standardising surface measurements"""

    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    AGAGE = "AGAGE"
    ICOS = "ICOS"
    NOAA = "NOAA"
    BEACO2N = "BEACO2N"
    NPL = "NPL"
    OPENGHG = "OPENGHG"
    CO2_GAMES = "CO2_GAMES"


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

    ACRG_ORG = "ACRG_ORG"
    PARIS = "PARIS"
    FLEXPART = "FLEXPART"  # This is an alias for PARIS


class ObsTypes(Enum):
    """ """

    SURFACE = "ObsSurface"
    COLUMN = "ObsColumn"
    MOBILE = "ObsMobile"


class FluxTimeseriesTypes(Enum):
    """For standardising one dimensional timeseries data"""

    CRF = "Crf"


class EulerianModelTypes(Enum):
    """For standardising eulerian model data"""

    OPENGHG = "OPENGHG"


class DataTypes(Enum):
    CRDS = "CRDS"
    GCWERKS = "GCWERKS"
    AGAGE = "AGAGE"
    NOAA = "NOAA"
    ICOS = "ICOS"
    BEACO2N = "BEACO2N"
    Footprints = "Footprints"
    NPL = "NPL"
    OPENGHG = "OPENGHG"
    INTEM = "INTEM"


class BoundaryConditions(Enum):
    OPENGHG = "OPENGHG"
