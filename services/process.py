# from typing import Dict

# from Acquire.Client import PAR, Authorisation
# from Acquire.Service import get_this_service
# from openghg.store import ObsSurface
# from tempfile import TemporaryDirectory


# def process(args: Dict) -> Dict:
#     """Process uploaded data files and store in the object store

#     Args:
#         args: Dictionary of JSON serialised objects to be
#         used by retrieve functions
#     Returns:
#         dict: Dictionary of results of retrieve
#     """
#     data_type = args["data_type"]
#     data_type = data_type.upper()

#     data_par = PAR.from_data(args["par"]["data"])
#     data_secret = args["par_secret"]["data"]

#     auth = args["authorisation"]
#     authorisation = Authorisation.from_data(auth)

#     # Verify that this process had authorisation to be called
#     authorisation.verify("process")

#     openghg = get_this_service(need_private_access=True)

#     data_secret = openghg.decrypt_data(data_secret)
#     data_filename = data_par.resolve(secret=data_secret)
#     # Here we're downloading the data to a temporary directory
#     # Be good if we could load it directly from the object store
#     with TemporaryDirectory() as tmp_dir:
#         data_file = data_filename.download(directory=tmp_dir)

#         site = args["site"]
#         network = args["network"]
#         instrument = args.get("instrument")
#         inlet = args.get("inlet")
#         overwrite = args.get("overwrite", False)

#         if data_type in ("GCWERKS", "GC"):
#             precision_par = PAR.from_data(args["par"]["precision"])
#             precision_secret = args["par_secret"]["precision"]
#             precision_secret = openghg.decrypt_data(precision_secret)
#             precision_filename = precision_par.resolve(precision_secret)
#             precision_file = precision_filename.download(directory=tmp_dir)

#             data_file = data_file, precision_file

#         results = ObsSurface.read_file(
#             filepath=data_file,
#             data_type=data_type,
#             site=site,
#             network=network,
#             instrument=instrument,
#             inlet=inlet,
#             overwrite=overwrite,
#         )

#         return {"results": results}
