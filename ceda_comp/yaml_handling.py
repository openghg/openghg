import json
import yaml

def get_ceda_yaml(*args):
    """ Creates a YAML object for export for a CEDA upload

        Args:
            site (str): Name of site
            instrument (str): Name of instrument
            height (str): Height of instrument
            date_range (tuple): Start, end Python datetime objects
        Returns:
            str: Dictionary object serialised to YAML document
    """

    # YAML versions in example files look a lot like JSON instad of the YAML
    # that yaml.dump(data) creates. Is this a different version of yaml?
    # Load in JSON for storing data for CEDA compliance
    # JSON feels cleaner to work with/read here
    with open("ceda_compliance.json", "r") as f:
        ceda_comp = json.load(f)

    site = "bsd"

    site_title = ceda_comp[site]["title"]
    site_description = ceda_comp[site]["description"]
    
    data = {}
    # Lookup the site data in ceda_compliance dictionary for site_description etc
    data["title"] = site_title
    # Get this from a YAML file that has each site saved
    data["description"] = site_description
    # Similarly load from YAML?
    # Where site_authors is a dict containing authors
    data["authors"] = [{"firstname": "HUGS", "surname": "Cloud"}, 
                        {"firstname": "", "surname": ""}, 
                        {"firstname": "", "surname": ""}],

    # Here we'll have to add in the degree notation?
    data["bbox"] = {"north": ceda_comp[site]["latitude"], "south": "",
                    "east": "", "west": ceda_comp[site]["longitude"]}

    data["time_range"] = {"start": "2014-05-02 00:00:00",
                        "end": "2014-31-12 23:00:00"}

    # These can be loaded in from YAML / JSON
    data["lineage"] = ceda_comp[site]["lineage"]
    data["quality"] = ceda_comp[site]["quality"]

    data["docs"] = [{"title": site_title, "url": ceda_comp[site]["url"]}]

    data["project"] = {"catalogue_url": ceda_comp[site]["catalogue_url"], "title": site_title, "description": site_description,
                        "PI": {"firstname": "HUGS", "lastname": "Cloud"}, "funder": "NERC", "grant_number": "HUGS_Grant"}

    data["instrument"] = {"catalogue_url": "Instrument url", 
                        "title": "Instrument title", 
                        "description": "Instrument description"}

    # This is empty in the examples sent
    data["computation"] = {"catalogue_url": "",
                            "title": "",
                            "description": ""}

    with open("test_bsd.yaml", "w") as f:
        yaml.dump(data, f)

if __name__ == "__main__":
    get_ceda_yaml("t")


    