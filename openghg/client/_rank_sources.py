from openghg.dataobjects import RankSources
from openghg.util import running_in_cloud


def rank_sources(site: str, species: str, service_url: str = None) -> RankSources:
    """Retrieve datasources for a specific site and species. Returns a RankSources
    object that can be used to set and modify ranking data.

    Args:
        site: Site code
        species: Species
    Returns:
        RankSources: A RankSources object
    """
    cloud = running_in_cloud()
    ranker = RankSources(cloud=cloud, service_url=service_url)
    ranker.get_sources(site=site, species=species)

    return ranker
