from typing import Dict


def search(args: Dict) -> Dict:
    from openghg.processing import search as openghg_search

    skip_ranking = str(args["skip_ranking"]).lower()

    if skip_ranking == "true":
        args["skip_ranking"] = True
    else:
        args["skip_ranking"] = False

    results = openghg_search(**args)

    results_data = results.to_data()

    return {"results": results_data}
