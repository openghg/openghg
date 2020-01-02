import pytest

from HUGS.Modules import ThamesBarrier


def test_site_attributes():
    tb = ThamesBarrier()

    site_attributes = tb.site_attributes()

    assert site_attributes["data_owner"] == "Valerio Ferracci"
    assert site_attributes["data_owner_email"] == "V.Ferracci@cranfield.ac.uk"
    assert site_attributes["Notes"] == "~5m above high tide water level, in tidal region of the Thames"
    assert site_attributes["inlet_height_magl"] == "5 m"
    assert site_attributes["instrument"] == "Picarro G2401"
