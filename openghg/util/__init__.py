"""
    Utility functions for OpenGHG
"""

from ._domain import convert_longitude, find_domain
from ._download import download_data, parse_url_filename
from ._export import to_dashboard, to_dashboard_mobile
from ._file import (
    compress,
    compress_json,
    compress_str,
    decompress,
    decompress_json,
    decompress_str,
    get_datapath,
    get_logfile_path,
    load_emissions_database_parser,
    load_emissions_parser,
    load_json,
    load_surface_parser,
    read_header,
)
from ._hashing import hash_bytes, hash_file, hash_retrieved_data, hash_string
from ._species import check_lifetime_monthly, molar_mass, species_lifetime, synonyms
from ._strings import clean_string, is_number, remove_punctuation, to_lowercase
from ._time import (
    check_date,
    check_nan,
    closest_daterange,
    combine_dateranges,
    create_daterange,
    create_daterange_str,
    create_frequency_str,
    daterange_contains,
    daterange_from_str,
    daterange_overlap,
    daterange_to_str,
    find_daterange_gaps,
    find_duplicate_timestamps,
    first_last_dates,
    parse_period,
    relative_time_offset,
    sanitise_daterange,
    split_daterange_str,
    split_encompassed_daterange,
    time_offset,
    time_offset_definition,
    timestamp_epoch,
    timestamp_now,
    timestamp_tzaware,
    trim_daterange,
    valid_daterange,
)
from ._tutorial import bilsdale_datapaths, retrieve_example_data
from ._util import (
    find_matching_site,
    multiple_inlets,
    pairwise,
    running_in_cloud,
    running_locally,
    running_on_hub,
    site_code_finder,
    unanimous,
    verify_site,
)
