from openghg.util import bilsdale_datapaths


def test_bilsdale_data():
    paths = bilsdale_datapaths()

    names = [p.name for p in paths]
    names.sort()

    assert names == ["bsd.picarro.1minute.108m.min.dat", "bsd.picarro.1minute.248m.min.dat", "bsd.picarro.1minute.42m.min.dat"]
