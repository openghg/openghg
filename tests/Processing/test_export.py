from collections import OrderedDict
import json
from pathlib import Path
import os
from pathlib import Path
import pytest
import tempfile
import yaml
from xarray import open_dataset

from HUGS.Processing import get_ceda_file
from HUGS.Modules import CRDS, GC
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



# def test_export_compliant_without_file():
#     data_path = Path(__file__).resolve().parent.joinpath("../data/cf_compliant.nc")

#     test_data = open_dataset(data_path)

#     results, data = export_compliant(data=test_data)

#     correct_results = {'FATAL': 0, 'ERROR': 0, 'WARN': 1, 'INFO': 2, 'VERSION': 6}

#     assert results == correct_results


# def test_export_non_compliant_without_file():
#     data_path = Path(__file__).resolve().parent.joinpath("../data/non_cf_compliant.nc")

#     test_data = open_dataset(data_path)

#     with pytest.raises(ValueError):
#         results = export_compliant(data=test_data)


# def test_export_compliant_with_file(tmpdir):
#     data_path = Path(__file__).resolve().parent.joinpath("../data/cf_compliant.nc")

#     test_data = open_dataset(data_path)

#     tmp_filepath = Path(tmpdir).joinpath("test_compliance.nc")

#     results = export_compliant(data=test_data, filepath=tmp_filepath)

#     correct_results = {'FATAL': 0, 'ERROR': 0, 'WARN': 1, 'INFO': 2, 'VERSION': 6}

#     assert results == correct_results

#     written_ds = open_dataset(tmp_filepath)

#     assert written_ds.equals(test_data)