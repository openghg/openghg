import numpy as np
from openghg.standardise import summary_source_formats, summary_site_codes


def test_summarise_format():
    """Test to check summary_source_formats() function is producing expected output"""
    summary_df = summary_source_formats()

    # Check subset of columns in DataFrame
    expected_columns = ["Long name", "Source format", "Platform"]
    for col in expected_columns:
        assert col in summary_df

    # Check expected source format is returned for specific value
    name = "Gosan, Korea"
    expected_source_format = "GCWERKS"

    selection = summary_df[summary_df["Long name"] == name]
    assert selection["Source format"].values == expected_source_format


def test_summarise_site():
    """Test to check summary_site_codes() function is producing expected output"""
    summary_df = summary_site_codes()

    # Check subset of columns in DataFrame
    expected_columns = ["Network",
                        "Long name",
                        "Latitude",
                        "Longitude",
                        "Station height (masl)",
                        "Inlet heights"]

    for col in expected_columns:
        assert col in summary_df

    # Check expected long name is returned for specific site and network
    site_code = "BSD"
    network = "DECC"
    expected_long_name = "Bilsdale, UK"

    selection_all = summary_df.loc[site_code]
    selection = selection_all[selection_all["Network"] == network]
    assert selection["Long name"].values == expected_long_name

