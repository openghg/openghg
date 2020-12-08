from openghg.modules import Datasource, ObsSurface
from openghg.processing import DataTypes
from openghg.util import load_object


def get_sources(args):
    """ Get the Datasources associated with the specified species at a specified site

        Args:
            args (dict): Dictionary containing site and species keys
        Returns:
            dict: Dictionary of 
    """
    try:
        site = args["site"]
    except KeyError:
        # TODO - created a SiteError error type to raise here
        raise KeyError("Site must be specified")

    try:
        species = args["species"]
    except KeyError:
        raise KeyError("Species must be specified")

    obs = ObsSurface.load()
   
    datasource_uuids = obs.datasources()
    # Shallow load the Datasources (only get their JSON metadata)
    datasources = [Datasource.load(uuid=uuid, shallow=True) for uuid in datasource_uuids]

    matching_sources = [d for d in datasources if d.search_metadata(search_terms=[site, species], find_all=True)]

    def name_str(d):
        return "_".join([d.species(), d.site(), d.inlet(), d.instrument()])

    unranked = {name_str(d): {"rank": d.rank(), "data_range": d.daterange_str(), "uuid": d.uuid()} for d in matching_sources}

    return unranked
