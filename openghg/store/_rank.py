from __future__ import annotations

from typing import TYPE_CHECKING

from openghg.util import running_locally

if TYPE_CHECKING:
    from openghg.dataobjects import RankSources


def rank_sources(site: str, species: str, service_url: str = None) -> RankSources:
    """Retrieve datasources for a specific site and species. Returns a RankSources
    object that can be used to set and modify ranking data.

    Args:
        site: Site code
        species: Species
    Returns:
        RankSources: A RankSources object
    """
    from openghg.dataobjects import RankSources

    hub_or_cloud = not running_locally()
    ranker = RankSources(cloud=hub_or_cloud, service_url=service_url)
    ranker.get_sources(site=site, species=species)

    return ranker
