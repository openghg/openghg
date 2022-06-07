import pytest
import numpy as np
from openghg.util import find_domain
from openghg.util._domain import _get_coord_data


def test_find_domain():
    """Test find_domain function can be used to find correct lat, lon values"""
    domain = "EUROPE"

    latitude, longitude = find_domain(domain)

    assert latitude[0] == 1.072900009155273438e+01
    assert latitude[-1] == 7.905699920654296875e+01
    assert longitude[0] == -9.790000152587890625e+01
    assert longitude[-1] == 3.938000106811523438e+01


def test_find_domain_missing():
    """Test find_domain function returns an error if domain is not present"""
    domain = "FAKE"

    with pytest.raises(ValueError) as e_info:
        find_domain(domain)


def test_create_coord():
    """Test coordinate values can be created from range and increment"""
    coord = "coordinate"
    domain = "TEST"

    range = [-10, 10]
    increment = 0.1
    data = {"coordinate_range": range,
            "coordinate_increment": increment}

    coordinate = _get_coord_data(coord, data, domain)

    assert np.isclose(coordinate[0], range[0])
    assert np.isclose(coordinate[-1], range[-1])

    av_increment = (coordinate[1:] - coordinate[:-1]).mean()
    assert np.isclose(av_increment, increment)
