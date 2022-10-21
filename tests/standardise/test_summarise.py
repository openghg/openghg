from openghg.standardise import summary_source_format, summary_site_codes


def test_summarise_format():
    """Test to check summary_source_formats() function is producing expected output"""
    summary_df = summary_source_format()

    # Check subset of columns in DataFrame
    expected_columns = ["Long name", "Source format", "Platform"]
    for col in expected_columns:
        assert col in summary_df

    # Check expected source format is returned for specific value
    name = "mtecimone"
    expected_data_type = "GCWERKS"

    selection = summary_df[summary_df["Long name"] == name]
    assert selection["Data type"].values == expected_data_type


def test_summarise_site():
    """Test to check summary_site_codes() function is producing expected output"""
    summary_df = summary_site_codes()

    # Check subset of columns in DataFrame
    expected_columns = ["network",
                        "long_name",
                        "latitude",
                        "longitude",
                        "height_station_masl",
                        "heights"]

    for col in expected_columns:
        assert col in summary_df

    # Check expected long name is returned for specific site and network
    site_code = "BSD"
    network = "DECC"
    expected_long_name = "Bilsdale, UK"

    selection_all = summary_df.loc[site_code]
    selection = selection_all[selection_all["network"] == network]
    assert selection["long_name"].values == expected_long_name

