import os

import pandas as pd
import pytest
from Acquire.Client import PAR, Authorisation, Drive, Service, StorageCreds
from openghg.client import Search
from openghg.objectstore import get_local_bucket


@pytest.fixture(scope="session")
def tempdir(tmpdir_factory):
    d = tmpdir_factory.mktemp("")
    return str(d)


@pytest.fixture(autouse=True)
def crds(authenticated_user):
    get_local_bucket(empty=True)
    creds = StorageCreds(user=authenticated_user, service_url="storage")
    drive = Drive(creds=creds, name="test_drive")
    filepath = os.path.join(
        os.path.dirname(__file__),
        "../../../tests/data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat",
    )
    filemeta = drive.upload(filepath)

    par = PAR(location=filemeta.location(), user=authenticated_user)

    hugs = Service(service_url="hugs")
    par_secret = openghg.encrypt_data(par.secret())

    auth = Authorisation(resource="process", user=authenticated_user)

    args = {
        "authorisation": auth.to_data(),
        "par": {"data": par.to_data()},
        "par_secret": {"data": par_secret},
        "data_type": "CRDS",
        "source_name": "bsd.picarro.1minute.248m",
    }

    openghg.call_function(function="process", args=args)


@pytest.mark.skip(reason="Need to fix dependence on Acquire")
def test_retrieve(authenticated_user, crds):
    search_term = "co"
    location = "bsd"

    search_obj = Search(service_url="hugs")

    search_results = search_obj.search(species=search_term, locations=location)

    key = list(search_results.keys())[0]

    result = search_obj.download(selected_keys=key)
    data = result[0]

    del data.attrs["File created"]

    expected_attributes = {
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "248m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "auto@hugs-cloud.com",
        "species": "co",
        "Calibration_scale": "unknown",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "Bilsdale, UK",
        "station_height_masl": 380.0,
    }

    assert data["time"][0] == pd.Timestamp("2014-01-30T10:52:30")
    assert data["co"][0] == pytest.approx(204.62)
    assert data.attrs == expected_attributes

    # # Here we get some JSON data that can be converted back into a DataFrame
    # df = read_json(data["bsd_co"])

    # head = df.head(1)

    # assert head.first_valid_index() == Timestamp("2014-01-30 10:52:30")
    # assert head["co count"].iloc[0] == 204.62
    # assert head["co stdev"].iloc[0] == 6.232
    # assert head["co n_meas"].iloc[0] == 26
