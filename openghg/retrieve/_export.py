""" This file contains functions used to export data in compliant formats for archiving with
    facilities such as CEDA


"""

__all__ = ["get_ceda_file"]


def get_ceda_file(  # type: ignore
    filepath=None,
    site=None,
    instrument=None,
    height=None,
    write_yaml=False,
    date_range=None,
):
    """Creates a JSON (with a yaml extension for CEDA reasons) object
    for export for a CEDA upload

    Args:
        filepath (str or Path, default=None): Path for file output. If file not passed a
        dictionary is returned
        site (str, default=None): Three letter site code (such as BSD for Bilsdale)
        instrument (str, default=None): Name of instrument
        height (str, default=None): Height of instrument
        write_yaml (bool, default=False): If True write to YAML, otherwise JSON file is written
        date_range (tuple, default=None): Start, end Python datetime objects
    Returns:
        dict: Dictionary for upload to CEDA

    """
    import json
    from pathlib import Path

    import yaml
    from openghg.util import get_datapath

    if filepath:
        filepath = Path(filepath)

    compliance_file = "ceda_compliance.json"
    compliance_path = get_datapath(filename=compliance_file)

    # Load in JSON for storing data for CEDA compliance
    # JSON feels cleaner to work with/read here
    with open(compliance_path) as f:
        ceda_comp = json.load(f)

    if not site:
        raise ValueError("Site must be given")

    site = site.upper()

    # Ensure height is in the format we want
    height = str(height).lower().replace(" ", "")
    if not height.endswith("m"):
        height = f"{height}m"

    site_title = ceda_comp[site]["title"]
    site_description = ceda_comp[site]["description"]
    site_instrument = ceda_comp[site]["instruments"][instrument]

    data = {}
    # Lookup the site data in ceda_compliance dictionary for site_description etc
    data["title"] = site_title
    # Get this from a YAML file that has each site saved
    data["description"] = site_description
    # Similarly load from YAML?yaml
    # Where site_authors is a dict containing authors
    data["authors"] = [
        {"firstname": "OpenGHG", "surname": "Cloud"},
        {"firstname": "", "surname": ""},
        {"firstname": "", "surname": ""},
    ]

    # Here we'll have to add in the degree notation?
    data["bbox"] = {
        "north": ceda_comp[site]["latitude"],
        "south": "",
        "east": "",
        "west": ceda_comp[site]["longitude"],
    }

    data["time_range"] = {"start": "2014-05-02 00:00:00", "end": "2014-31-12 23:00:00"}

    # These can be loaded in from YAML / JSON
    data["lineage"] = ceda_comp[site]["lineage"]
    data["quality"] = ceda_comp[site]["quality"]

    data["docs"] = [{"title": site_title, "url": ceda_comp[site]["url"]}]

    data["project"] = {
        "catalogue_url": ceda_comp[site]["catalogue_url"],
        "title": site_title,
        "description": site_description,
        "PI": {"firstname": "OpenGHG", "lastname": "Cloud"},
        "funder": "NERC",
        "grant_number": "OpenGHG_Grant",
    }

    data["instrument"] = {
        "catalogue_url": site_instrument["catalogue_url"],
        "title": site_instrument["title"],
        "description": site_instrument["description"],
        "height": height,
    }

    # This is empty in the examples sent
    data["computation"] = {"catalogue_url": "", "title": "", "description": ""}

    if filepath:
        with open(filepath, "w") as f:
            if write_yaml:
                yaml.dump(data, stream=f, indent=4)
            else:
                json.dump(data, fp=f, indent=4)

        return None
    else:
        return data


# def export_compliant(data, filepath=None):
#     """ Check the passed data is CF compliant and if a filepath is passed
#         export to a NetCDF file

#         Args:
#             data (xarray.Dataset): Data to export
#             filepath (str): Path to export data file
#         Returns:
#             dict or tuple (dict, xarray.Dataset): Results dictionary or results dictionary and data
#             if no filepath for writing is passed
#     """
#     from cfchecker import chkFiles
#     from contextlib import redirect_stdout
#     import io
#     from pathlib import Path
#     import subprocess
#     import tempfile

#     # If we don't have a filepath to write the NetCDF to we write to a temporary file
#     # TODO - modify cf-checker to allow checking of Datasets?
#     if filepath is None:
#         tmpfile = tempfile.NamedTemporaryFile(suffix=".nc")
#         check_file = tmpfile.name
#     else:
#         filepath = Path(filepath).absolute()
#         check_file = str(filepath)

#     data.to_netcdf(check_file)

#     # Here we capture the stdout from the chkFiles function which will include any useful error messages
#     c = io.StringIO()
#     with redirect_stdout(c):
#         results = chkFiles(files=check_file, silent=False)

#     results = dict(results)

#     try:
#         tmpfile.close()
#     except NameError:
#         pass

#     stdout_capture = c.getvalue()

#     # Return the useful error messages if we get any
#     if results["FATAL"] or results["ERROR"]:
#         # Clean the error messages
#         stdout_capture = stdout_capture.replace("\n", " ")
#         # raise ValueError(f"{results["FATAL"]}") # fatal and {results["ERROR"]} non-fatal errors found")
#         raise ValueError(f"{results['FATAL']} fatal and {results['ERROR']} non-fatal errors found.\
#              Please make changes to ensure your file is compliant.\n\n {stdout_capture}")

#     if filepath is None:
#         return results, data
#     else:
#         return results
