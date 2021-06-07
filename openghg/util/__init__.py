"""
    Utility functions for OpenGHG
"""

from ._util import (
    create_uuid,
    get_datapath,
    load_json,
    load_object,
    read_header,
    unanimous,
    valid_site,
    is_number,
    to_lowercase,
    to_defaultdict,
    pairwise,
)

from ._hashing import hash_string, hash_file
from ._strings import clean_string

from ._compliance import compliant_string

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
)

from ._tutorial import bilsdale_datapaths
