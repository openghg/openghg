import pytest
import pandas as pd
from openghg.retrieve.icos import retrieve
from helpers import get_icos_test_file

# import icoscp.station.station
from icoscp.station import station
from icoscp.station.station import Station


def test_icos_retrieve_no_local_no_species(mocker):
    fake_data = mocker.Mock
    mock_pid_file = get_icos_test_file(filename="test_pids_icos.csv.gz")
    datas = pd.read_csv(mock_pid_file)
    fake_data.return_value = datas

    mocker.patch.object(station, "get", mocker.Mock)
    mocker.patch.object(Station, "data", fake_data)

    retrieve(site="ABC")

    return
    # Create a mock Station the function can read from
    # datetime.datetime.today.return_value = tuesday

    # st = create_station()

    # mock_station = station.Station

    # stat = station.get(stationId=site.upper())
    # data_pids = stat.data(level=data_level)
    # dobj = Dobj(dobj_url)


#

# requests_mock.post(ANY, json=mock_return, status_code=200)
# requests_mock.get("https://www.example.com", content=mock_data, status_code=503)
