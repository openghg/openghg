import json
from pathlib import Path
import pytest
import tempfile
import yaml

from HUGS.Processing import create_upload_file

def test_create_upload_file_raises_no_args():
    with pytest.raises(ValueError):
        create_upload_file()

def test_get_upload_file_json_file_output(tmpdir):
    tmp_json = Path(tmpdir).joinpath("test.json")
    create_upload_file(filepath=tmp_json, site="bsd", instrument="instrument_A", height="180m", write_yaml=False)

    assert tmp_json.exists()

    # Check we can read it in correctly
    with open(tmp_json, "r") as f:
        data = json.load(f)

    assert data["title"] == 'Bilsdale Tall Tower'
    assert data["authors"] == [{'firstname': 'HUGS', 'surname': 'Cloud'}, {'firstname': '', 'surname': ''},
                               {'firstname': '', 'surname': ''}]
    assert data["bbox"] == {'north': 50, 'south': '', 'east': '', 'west': 0}


def test_create_upload_file_yaml_file_output(tmpdir):
    tmp_yaml = Path(tmpdir).joinpath("test.yaml")
    create_upload_file(filepath=tmp_yaml, site="bsd", instrument="instrument_A", height="180m", write_yaml=True)

    assert tmp_yaml.exists()

    # Make sure we can read it in correctly
    with open(tmp_yaml, "r") as f:
        data = yaml.safe_load(f)
    
    assert data["bbox"] == {'north': 50, 'south': '', 'east': '', 'west': 0}
    assert data["instrument"] == {'catalogue_url': 'Instrument url',
                                  'title': 'instrument_a', 'description': 'an_instrument',
                                  'catalogue_url': 'example.com', 'height': '180m'}

# With no filepath passed we get a dictionary returned
def test_create_upload_file_return_dictionary():
    data = create_upload_file(site="bsd", instrument="instrument_A", height="180m")

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


def test_create_upload_file_height_correctness():
    data = create_upload_file(site="bsd", instrument="instrument_A", height="180")

    assert data["instrument"]["height"] == "180m"

    data = create_upload_file(site="bsd", instrument="instrument_A", height="180 m")

    assert data["instrument"]["height"] == "180m"

    data = create_upload_file(site="bsd", instrument="instrument_A", height="100M")

    assert data["instrument"]["height"] == "100m"


