import json
from pathlib import Path
import os
from pathlib import Path
import pytest
import tempfile
import yaml

from HUGS.Processing import get_ceda_file, export_compliant
from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket, get_object_names

def test_get_ceda_file_raises_no_args():
    with pytest.raises(ValueError):
        get_ceda_file()

def test_get_upload_file_json_file_output(tmpdir):
    tmp_json = Path(tmpdir).joinpath("test.json")
    get_ceda_file(filepath=tmp_json, site="bsd", instrument="instrument_A", height="180m", write_yaml=False)

    assert tmp_json.exists()

    # Check we can read it in correctly
    with open(tmp_json, "r") as f:
        data = json.load(f)

    assert data["title"] == 'Bilsdale Tall Tower'
    assert data["authors"] == [{'firstname': 'HUGS', 'surname': 'Cloud'}, {'firstname': '', 'surname': ''},
                               {'firstname': '', 'surname': ''}]
    assert data["bbox"] == {'north': 50, 'south': '', 'east': '', 'west': 0}


def test_get_ceda_file_yaml_file_output(tmpdir):
    tmp_yaml = Path(tmpdir).joinpath("test.yaml")
    get_ceda_file(filepath=tmp_yaml, site="bsd", instrument="instrument_A", height="180m", write_yaml=True)

    assert tmp_yaml.exists()

    # Make sure we can read it in correctly
    with open(tmp_yaml, "r") as f:
        data = yaml.safe_load(f)
    
    assert data["bbox"] == {'north': 50, 'south': '', 'east': '', 'west': 0}
    assert data["instrument"] == {'catalogue_url': 'Instrument url',
                                  'title': 'instrument_a', 'description': 'an_instrument',
                                  'catalogue_url': 'example.com', 'height': '180m'}

# With no filepath passed we get a dictionary returned
def test_get_ceda_file_return_dictionary():
    data = get_ceda_file(site="bsd", instrument="instrument_A", height="180m")

    assert data["title"] == 'Bilsdale Tall Tower'
    assert data["authors"] == [{'firstname': 'HUGS', 'surname': 'Cloud'}, {'firstname': '', 'surname': ''},
                               {'firstname': '', 'surname': ''}]
    assert data["bbox"] == {'north': 50, 'south': '', 'east': '', 'west': 0}
    assert data["instrument"] == {'catalogue_url': 'Instrument url', 
                                  'title': 'instrument_a', 'description': 'an_instrument',
                                  'catalogue_url': 'example.com', 'height': '180m'}
    assert data["project"] == {'catalogue_url': 'http: //www.metoffice.gov.uk/research/monitoring/atmospheric-trends/sites/bilsdale', 
                                'title': 'Bilsdale Tall Tower', 
                                'description': 'Bilsdale (BSD) tall tower is in a remote area of the North York Moors National Park and is the first monitoring site in the northeast region of England.', 
                                'PI': {'firstname': 'HUGS', 'lastname': 'Cloud'}, 
                                'funder': 'NERC', 'grant_number': 'HUGS_Grant'}


def test_get_ceda_file_height_correctness():
    data = get_ceda_file(site="bsd", instrument="instrument_A", height="180")

    assert data["instrument"]["height"] == "180m"

    data = get_ceda_file(site="bsd", instrument="instrument_A", height="180 m")

    assert data["instrument"]["height"] == "180m"

    data = get_ceda_file(site="bsd", instrument="instrument_A", height="100M")

    assert data["instrument"]["height"] == "100m"



def test_export_compliant_without_file():
    crds = CRDS.load()

    bucket = get_local_bucket(empty=True)
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "hfd.picarro.1minute.100m_min.dat"

    filepath = Path(__file__).parent.resolve().joinpath(test_data, filename)

    # filepath = os.path.join(dir_path, test_data, filename)

    gas_data = crds.read_data(data_filepath=filepath, site="hfd")

    data = crds.assign_attributes(data=gas_data, site="hfd")

    # print(data.keys())

    compliant_data = export_compliant(data=data["ch4"]["data"])

    # with tempfile.NamedTemporaryFile() as tmpfile:


