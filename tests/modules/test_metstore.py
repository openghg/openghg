import pytest
from openghg.modules import METStore
from timeit import default_timer as timer

def test_retrieve():

    start = timer()
    met = METStore.retrieve(site="CGO", network="AGAGE", years="2012")
    elapsed = timer() - start

    print(f"To download a whole year's data it took : {elapsed} s")

