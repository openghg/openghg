"""
    Utility functions for OpenGHG
"""

from ._cli import cli
from ._combine import combine_and_elevate_inlet, combine_data_objects, combine_multisite
from ._data_level import format_data_level
from ._domain import (
    get_domain_info,
    find_domain,
    find_coord_name,
    convert_lon_to_180,
    convert_lon_to_360,
    convert_internal_longitude,
    cut_data_extent,
    align_lat_lon,
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
    load_json,
    load_internal_json,
    load_standardise_parser,
    load_transform_parser,
    read_header,
    check_function_open_nc,
    permissions,
)
from ._function_inputs import split_function_inputs
from ._hashing import hash_bytes, hash_file, hash_retrieved_data, hash_string
from ._inlet import format_inlet, extract_height_name
from ._metadata_util import (
    null_metadata_values,
    not_set_metadata_values,
    remove_null_keys,
    check_number_match,
    check_str_match,
    check_value_match,
    check_not_set_value,
    get_overlap_keys,
    merge_dict,
)
from ._site import get_site_info, sites_in_network
from ._species import (
    get_species_info,
    synonyms,
    species_lifetime,
    check_lifetime_monthly,
    check_species_lifetime,
    check_species_time_resolved,
    molar_mass,
)
from ._strings import (
    clean_string,
    extract_float,
    is_number,
    remove_punctuation,
    to_lowercase,
    check_and_set_null_variable,
)
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
    dates_overlap,
    daterange_to_str,
    evaluate_sampling_period,
    find_daterange_gaps,
    find_duplicate_timestamps,
    first_last_dates,
    in_daterange,
    parse_period,
    dates_in_range,
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
from ._user import (
    create_config,
    get_user_id,
    get_user_config_path,
    read_local_config,
    check_config,
)
from ._util import (
    find_matching_site,
    multiple_inlets,
    pairwise,
    site_code_finder,
    sort_by_filenames,
    unanimous,
    verify_site,
)
from ._versions import show_versions, check_if_need_new_version
