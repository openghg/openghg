import argparse
import json
from pathlib import Path

""" Controls a job running on a local / cloud HPC cluster

"""

def data_watchdog():
    """ Function that watches for completion of job

        WIP: Unsure how this will be implemented currently.
        Either watching for a file created at output or just upload
        data to the cloud drive as we go?

    """
    pass


parser = argparse.ArgumentParser(description='Run and watch a job on a HPC resource')
parser.add_argument("j", help="JSON data filename")
args = parser.parse_args()

json_filename = args.j
with open(json_filename, "r") as f:
    job_data = json.load(f)

job_name = json_data["job_name"]
script_file = json_data["script_filename"]
par_data = json_data["par"]

# Make the job folders at the location of this file
job_path = Path(__file__).joinpath(job_name)

folders = ["input", "output", "logs"]
for f in folders:
    fpath = job_path.join(f)
    fpath.mkdir(parents=True)

# Use the PAR (pre-authenticated request) to access the cloud drive
# where the input data has been uploaded (if needed) and output data will 
# be stored
par = PAR.from_data(par_data)
par_drive = par.resolve()

files = par_drive.list_files(dir="input")



# Download the input files





    

    


    
