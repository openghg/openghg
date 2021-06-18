from typing import Dict


def search(args: Dict) -> Dict:
    from openghg.processing import search as openghg_search

    results = openghg_search(**args)

    return {"results": results}
