
def job_runner(args):
    from HUGS.Jobs import SSHConnect
    from .test_fn import test_function
    import yaml

    # Take the PAR and write some data to it
    par = args["drive_par"]

    # Need a template script

    # Create a function that reads a JSON file that contains the data
    # Need to copy the PAR as JSON
    # Copy the created JSON, template script and any data to BC4
    # Run the script to create the job and watch the folder for data creation

    # SSH into BC4

    

    # Set a job running - get its ID ?

    # Get the job script to run and watch the output file and write that data to the correct PAR.
    # Will need to create a script on the fly?

    

    search_terms = args["search_terms"]
    locations = args["locations"]
    data_type = args["data_type"]vue

    results = _hugs_search(search_terms=search_terms, locations=locations, data_type=data_type,
                           start_datetime=start_datetime, end_datetime=end_datetime)

    return {"results": results}
