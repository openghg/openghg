from openghg.retrieve import search


def test_search_multi_inlet():
    res = search(species=["co2", "ch4"], data_type="surface", inlet="108m")

    print(res)


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

    data_types = set([m["data_type"] for m in res.metadata.values()])

    assert data_types == {"surface", "column"}

    res = search(species="co2")
    data_types = set([m["data_type"] for m in res.metadata.values()])

    assert data_types == {"emissions", "surface"}

    obs = res.retrieve_all()

    # Make sure the retrieval works correctly
    data_types = set([ob.metadata["data_type"] for ob in obs])

    assert data_types == {"emissions", "surface"}

    res = search(species="ch4", data_type=["surface"])

    assert len(res.metadata) == 7

    res = search(species="co2", data_type=["surface", "emissions"])

    assert len(res.metadata) == 7


def test_many_term_search():
    res = search(site=["bsd", "tac"], species=["co2", "ch4"], inlet=["42m", "100m"])

    assert len(res.metadata) == 4
    assert res.metadata

    sites = set([x["site"] for x in res.metadata.values()])
    assert sites == {"bsd", "tac"}

    species = set([x["species"] for x in res.metadata.values()])
    assert species == {"co2", "ch4"}

    inlets = set([x["inlet"] for x in res.metadata.values()])
    assert inlets == {"100m", "42m"}


def test_nonsense_terms():
    res = search(site="london", species="ch4")

    assert not res

    res = search(site="bsd", species="sparrow")

    assert not res
