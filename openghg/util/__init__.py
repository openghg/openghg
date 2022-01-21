"""
    Utility functions for OpenGHG
"""

from ._util import (
    unanimous,
    verify_site,
    pairwise,
    multiple_inlets,
    running_in_cloud
)

from ._hashing import hash_string, hash_file
from ._strings import clean_string, to_lowercase, remove_punctuation

from ._time import (
    timestamp_tzaware,
    timestamp_now,
    timestamp_epoch,
    daterange_from_str,
    daterange_to_str,
    create_daterange_str,
    create_daterange,
    daterange_overlap,
    combine_dateranges,
    split_daterange_str,
    closest_daterange,
    valid_daterange,
    find_daterange_gaps,
    trim_daterange,
    split_encompassed_daterange,
    daterange_contains,
    sanitise_daterange,
    check_nan,
    check_date,
)

from ._errors import InvalidSiteError, UnknownDataError, FunctionError
from ._tutorial import bilsdale_datapaths
from ._export import to_dashboard, to_dashboard_mobile
from ._file import get_datapath, load_json, load_surface_parser, read_header
