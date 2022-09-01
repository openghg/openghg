from __future__ import annotations

from typing import TYPE_CHECKING

from openghg.util import running_on_hub

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

    hub = running_on_hub()
    ranker = RankSources(cloud=hub)
    ranker.get_sources(site=site, species=species)

    return ranker
