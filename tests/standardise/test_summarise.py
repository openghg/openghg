from openghg.standardise import summary_data_types


def test_summarise():
    """Test to check summary_data_types() function is producing expected output"""
    summary_df = summary_data_types()

    # Check subset of columns in DataFrame
    expected_columns = ["Long name", "Data type", "Platform"]
    for col in expected_columns:
        assert col in summary_df

    # Check expected data type is returned for specific value
    name = "mtecimone"
    expected_data_type = "GCWERKS"

    selection = summary_df[summary_df["Long name"] == name]
    assert selection["Data type"].values == expected_data_type
