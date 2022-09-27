from openghg.retrieve import search


def test_search_site():
    res = search(site="bsd", species="co2", inlet="42m")

    expected = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "42m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2007",
        "long_name": "bilsdale",
        "inlet_height_magl": "42m",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "bilsdale, uk",
        "station_height_masl": 380.0,
    }

    key = next(iter(res.metadata))
    metadata = res.metadata[key]

    assert expected.items() <= metadata.items()

    res = search(site="bsd", species="co2", inlet="108m", instrument="picarro", calibration_scale="wmo-x2007")

    expected = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "108m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2007",
        "long_name": "bilsdale",
        "inlet_height_magl": "108m",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "bilsdale, uk",
        "station_height_masl": 380.0,
    }

    key = next(iter(res.metadata))
    metadata = res.metadata[key]

    assert expected.items() <= metadata.items()

    res = search(site="atlantis")

    assert not res


def test_multi_type_search():
    res = search(species="ch4")

    print(res.metadata)

    for m in res.metadata.values():
        print(m["data_type"])
