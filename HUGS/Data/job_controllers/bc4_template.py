import argparse
import json
import random

from pathlib import Path

from Acquire.Client import PAR

""" Controls a job running on a local / cloud HPC cluster

"""

def data_watchdog():
    """ Function that watches for completion of job

        WIP: Unsure how this will be implemented currently.
        Either watching for a file created at output or just upload
        data to the cloud drive as we go?

    """
    pass

def run():
    parser = argparse.ArgumentParser(description='Run and watch a job on a HPC resource')
    parser.add_argument("j", help="JSON data filename")
    args = parser.parse_args()

    json_filename = args.j
    with open(json_filename, "r") as f:
        job_data = json.load(f)

    job_name = job_data["job_name"]
    script_file = job_data["script_filename"]
    par_data = job_data["par"]
    
    with open("some_rands.txt", "w") as f:
        f.write(str(rands))

    # Make the job folders at the location of this file
    job_path = Path(__file__).resolve().parent.joinpath(job_name)

    folders = ["input", "output", "logs"]
    for f in folders:
        fpath = job_path.joinpath(f)
        fpath.mkdir(parents=True)

    # Use the PAR (pre-authenticated request) to access the cloud drive
    # where the input data has been uploaded (if needed) and output data will 
    # be stored
    par = PAR.from_data(par_data)
    par_drive = par.resolve()

    files = par_drive.list_files(dir="input")

    print(files)


if __name__ == "__main__":
    run()





# Download the input files



    


    
