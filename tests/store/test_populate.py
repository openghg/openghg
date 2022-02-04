from helpers import get_datapath
from openghg.store import add_noaa_obspack

def test_read_noaa_obspack_ch4():
    '''Test object store can be populated automatically when providing a NOAA
    ObsPack path. This contains:
     - directory structure: data/nc/
     - methane files only: "ch4_..."
     - surface flask files: "..._surface-flask_..."
     - aircraft pfp file: "..._aircraft-pfp_..." - this should be ignored for now
    '''
    data_directory = get_datapath("ObsPack_ch4", data_type="NOAA")
    out = add_noaa_obspack(data_directory)

    processed = out["processed"]

    # Check first file in folder has been processed
    filename1 = "ch4_esp_surface-flask_2_representative.nc"
    key1 = "ch4"
    assert filename1 in processed.keys()
    processed_1 = processed[filename1]
    assert key1 in processed_1.keys()

    # Check second file in folder has been processed
    filename2 = "ch4_spf_surface-flask_1_ccgg_Event.nc"
    key2 = "ch4_-2820m"  # Negative heights given in file!
    assert filename2 in processed.keys()
    processed_2 = processed[filename2]
    assert key2 in processed_2.keys()
    # TODO: update this test when we have thought a way around negative inlet heights
    # being supplied.


# TODO: Add test for multi-species / txt data when able to do so.
