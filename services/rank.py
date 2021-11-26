from typing import Dict
from openghg.store.base import Datasource
from openghg.store import ObsSurface


def set_rank(args: Dict) -> None:
    obs = ObsSurface.load()

    rank = args["rank"]
    uuid = args["uuid"]
    dateranges = args["dateranges"]
    overwrite = args["overwrite"]

    obs.set_rank(uuid=uuid, rank=rank, date_range=dateranges, overwrite=overwrite)


def clear_rank(args: Dict) -> None:
    obs = ObsSurface.load()

    uuid = args["uuid"]

    obs.clear_rank(uuid=uuid)


def get_sources(args: Dict) -> Dict:
    obs = ObsSurface.load()
    datasource_uuids = obs.datasources()
    rank_table = obs.rank_data()

    site = args["site"]
    species = args["species"]

    # Shallow load the Datasources (only get their JSON metadata)
    datasources = (Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids)

    matching_sources = [d for d in datasources if d.search_metadata(site=site, species=species)]

    if not matching_sources:
        return {}

    def name_str(d):
        return "_".join([d.species(), d.inlet(), d.instrument()])

    user_info = {
        name_str(d): {"rank_data": rank_table.get(d.uuid(), "NA"), "data_range": d.daterange_str()} for d in matching_sources
    }

    key_lookup = {name_str(d): d.uuid() for d in matching_sources}

    return {"user_info": user_info, "key_lookup": key_lookup}
