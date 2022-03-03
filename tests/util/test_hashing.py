from openghg import util


def test_hash_string():
    hash_string = util.hash_string

    h = hash_string("a_good_string")

    assert h == "6f5b8ce628133facca9028899ac55ae4ddedd8d0"
