"""
    Utility functions for OpenGHG
"""

from ._download import download_data, parse_url_filename
from ._export import to_dashboard, to_dashboard_mobile
from ._file import (
    get_datapath,
    load_json,
    load_surface_parser,
    load_emissions_parser,
    load_emissions_database_parser,
    read_header,
    compress,
    decompress,
)
from ._hashing import hash_file, hash_string, hash_retrieved_data, hash_bytes
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
    find_duplicate_timestamps,
    first_last_dates,
    sanitise_daterange,
    split_daterange_str,
    split_encompassed_daterange,
    timestamp_epoch,
    timestamp_now,
    timestamp_tzaware,
    trim_daterange,
    valid_daterange,
    time_offset_definition,
    parse_period,
    create_frequency_str,
    relative_time_offset,
    time_offset,
)
from ._tutorial import bilsdale_datapaths, retrieve_example_data
from ._util import (
    multiple_inlets,
    pairwise,
    running_in_cloud,
    unanimous,
    verify_site,
    find_matching_site,
    site_code_finder,
)
from ._species import synonyms, species_lifetime, check_lifetime_monthly, molar_mass
from ._domain import find_domain, convert_longitude
