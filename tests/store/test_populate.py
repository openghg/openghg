from helpers import get_surface_datapath, filt, make_keys
from openghg.store import add_noaa_obspack


def test_read_noaa_obspack_ch4():
    """Test object store can be populated automatically when providing a NOAA
    ObsPack path. This contains:
     - directory structure: data/nc/
     - methane files only: "ch4_..."
     - surface flask files: "..._surface-flask_..."
     - aircraft pfp file: "..._aircraft-pfp_..." - this should be ignored for now
    """
    data_directory = get_surface_datapath("ObsPack_ch4", source_format="NOAA")
    processed = add_noaa_obspack(data_directory=data_directory, store="user")

    # Check first file in folder has been processed
    filename1 = "ch4_esp_surface-flask_2_representative.nc"
    processed1 = filt(processed, file=filename1)
    assert processed1  # results exist for file=filename1
    assert processed1[0]["species"] == "ch4"

    # Check second file in folder has been processed
    filename2 = "ch4_spf_surface-flask_1_ccgg_Event.nc"
    key2 = "ch4_-2820m"  # Negative heights given in file!
    processed2 = filt(processed, file=filename2)
    assert processed2
    assert key2 in make_keys(processed2)
    # TODO: update this test when we have thought a way around negative inlet heights
    # being supplied.


# TODO: Add test for multi-species / txt data when able to do so.
