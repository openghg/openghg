import argparse
import json
import random
import subprocess

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
    par_secret = job_data["par_secret"]
    
    try:
        compilation_command = job_data["compilation_command"]
    except KeyError:
        compilation_command = None

    try:
        run_command = job_data["run_command"]
    except KeyError:
        run_command = f"sbatch {script_filename}"

    # Make the output folder
    fpath = job_path = Path(__file__).resolve().parent.joinpath("output")
    fpath.mkdir(parents=True)

    par = PAR.from_data(data=par_data)
    drive = par.resolve(secret=par_secret)

    # Download any data files and moved them to the input folders
    files = drive.list_files()
    for f in files:
        filename = f.filename()
        drive.download(filename=filename)

    # Split the compilation command
    if compilation_command is not None:
        cmd_list = compilation_command.split()
        # Run the compilation command and set the current working directory
        # to our application code location in "app"
        res = subprocess.run(cmd_list, stderr=True)
        
        if res.returncode != 0:
            raise subprocess.CalledProcessError("Compilation error : ", res.stderr)

    run_command = run_command.split()
    # Run the actual code
    runcmd_res = subprocess.run(run_command, stderr=True)

    if runcmd_res.returncode != 0:
        raise subprocess.CalledProcessError("Error running application : ", runcmd_res.stderr)

    # Use the PAR (pre-authenticated request) to access the cloud drive
    # where the input data has been uploaded (if needed) and output data will 
    # be stored
    filemeta = drive.upload("output/some_rands.txt", dir="output")

    files = drive.list_files()

    with open("returned_data.txt", "w") as w:
        w.write(str(filemeta))
        w.write(str(files))



if __name__ == "__main__":
    run()
