"""
    Utility functions for OpenGHG
"""
from ._cli import cli
from ._domain import (
    get_domain_info,
    find_domain,
    find_coord_name,
    convert_longitude,
    convert_internal_longitude,
    cut_data_extent,
)
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
    load_column_parser,
    load_column_source_parser,
    load_emissions_database_parser,
    load_emissions_parser,
    load_json,
    load_internal_json,
    load_surface_parser,
    read_header,
)
from ._hashing import hash_bytes, hash_file, hash_retrieved_data, hash_string
from ._inlet import format_inlet, extract_height_name
from ._site import get_site_info, sites_in_network
from ._species import get_species_info, check_lifetime_monthly, molar_mass, species_lifetime, synonyms
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
    in_daterange,
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
from ._user import create_config, get_user_id, get_user_config_path, read_local_config, check_config
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
from ._versions import show_versions
