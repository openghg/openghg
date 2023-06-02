from typing import Dict
from openghg.store.base import Datasource
from openghg.store import ObsSurface
from openghg.objectstore import get_bucket


def set_rank(args: Dict) -> None:
    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        rank = args["rank"]
        uuid = args["uuid"]
        dateranges = args["dateranges"]
        overwrite = args["overwrite"]

        obs.set_rank(uuid=uuid, rank=rank, date_range=dateranges, overwrite=overwrite)


def clear_rank(args: Dict) -> None:
    bucket = get_bucket()

    with ObsSurface(bucket=bucket) as obs:
        uuid = args["uuid"]

        obs.clear_rank(uuid=uuid)


def get_sources(args: Dict) -> Dict:
    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        datasource_uuids = obs.datasources()
        rank_table = obs.rank_data()

    site = args["site"]
    species = args["species"]

    # Shallow load the Datasources (only get their JSON metadata)
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    matching_sources = [d for d in datasources if d.search_metadata(site=site, species=species)]

    if not matching_sources:
        return {}

    user_info = {
        d.inlet(): {
            "rank_data": rank_table.get(d.uuid(), "NA"),
            "data_range": d.daterange_str(),
        }
        for d in matching_sources
    }

    key_lookup = {d.inlet(): d.uuid() for d in matching_sources}

    return {"user_info": user_info, "key_lookup": key_lookup}
