# from openghg.processing import scale_convert

# def test_scale_convert():
#     results = get_observations(site="hfd", species="co", start_date="2001-01-01", end_date="2015-01-01")

#     data = results[0].data

#     assert data["mf"][0] == pytest.approx(214.28)
#     assert data["co_variability"][0] == pytest.approx(4.081)
#     assert data["co_number_of_observations"][0] == 19.0

#     # # Fix the scale for test purposes
#     data.attrs["scale"] = "WMO-X2014A"

#     new_scale = "CSIRO94"

#     data = scale_convert(data=data, species="co", to_scale=new_scale)

#     assert data["mf"][0] == pytest.approx(902.755)
#     assert data["co_variability"][0] == pytest.approx(4.081)
#     assert data["co_number_of_observations"][0] == 19.0

#     assert data.attrs["scale"] == new_scale

#     results = get_observations(site="hfd", species="ch4", start_date="2001-01-01", end_date="2015-01-01")

#     data = results[0].data

#     assert data["mf"][0] == pytest.approx(1993.83)
#     assert data["ch4_variability"][0] == pytest.approx(1.555)
#     assert data["ch4_number_of_observations"][0] == 19.0

#     data.attrs["scale"] = "WMO-X2014A"

#     data = scale_convert(data=data, species="co", to_scale=new_scale)

#     assert data["mf"][0] == pytest.approx(8399.95)
#     assert data["ch4_variability"][0] == pytest.approx(1.555)
#     assert data["ch4_number_of_observations"][0] == 19.0