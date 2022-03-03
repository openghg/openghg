"""
    Utility functions for OpenGHG
"""

from ._export import to_dashboard, to_dashboard_mobile
from ._file import get_datapath, load_json, load_surface_parser, read_header
from ._hashing import hash_file, hash_string
from ._strings import clean_string, remove_punctuation, to_lowercase, is_number
from ._time import (
    check_date,
    check_nan,
    closest_daterange,
    combine_dateranges,
    create_daterange,
    create_daterange_str,
    daterange_contains,
    daterange_from_str,
    daterange_overlap,
    daterange_to_str,
    find_daterange_gaps,
    first_last_dates,
    sanitise_daterange,
    split_daterange_str,
    split_encompassed_daterange,
    timestamp_epoch,
    timestamp_now,
    timestamp_tzaware,
    trim_daterange,
    valid_daterange,
)
from ._tutorial import bilsdale_datapaths, retrieve_example_data
from ._util import multiple_inlets, pairwise, running_in_cloud, unanimous, verify_site
