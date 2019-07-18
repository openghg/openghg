
def retrieve(args):
	""" Calls the HUGS function to retrieve data stored at the given keyd
		and combine them into a single Pandas DataFrame for download / visualization

		Args:
			args (dict): Dictionary of arguments
		Returns:
			dict: Dictionary of results

	"""
	from HUGS.Processing import recombine_sections as _recombine_sections

	if "keys" in args:
		keys = args["keys"]
	else:
		raise KeyError("Keys required for data retrieval")

	if "return_type" in args:
		return_type = args["return_type"]
	else:
		return_type = "csv"

	results = _recombine_sections(keys)

	# Here we have to convert a number of dataframes to csv and
	# add them to a list, this feels very clunky but should work?

	# Convert dataframes to JSON here
	# JSON or just return binary data?
	results_list = []
	if return_type == "json":
		for key in results:
			results_list.append(results[key].to_json())
	else:
		# TODO - implement this
		results = False
	# Can select the return type of the data?
	# On download can select the type required

	# How will this work with returning a dataframe?
	return {"results" : results_list}