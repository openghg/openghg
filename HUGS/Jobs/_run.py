from datetime import datetime
import json
from pathlib import Path
import tempfile

from HUGS.Jobs import SSHConnect
from HUGS.Util import get_datapath

def run_job(username, hostname, job_data):
    """ Set a job to run on a HPC service

        Args:
            job_data (dict): Dictionary containing data needed to run the job
            such as the run command, number of nodes, number of tasks etc

            TODO - improve this
        Returns:
            dict: Dictionary of responses to commands executing when running the job
    """
    # Maybe this can be passed in / read from JSON depending on the service selected
    bc4_partitions = ['cpu_test', 'dcv', 'gpu', 'gpu_veryshort', 'hmem', 'serial', 'test', 'veryshort']

    name = job_data["name"]
    run_command = job_data["run_command"]
    partition = job_data["partition"]
    
    if partition not in bc4_partitions:
        raise ValueError(f"Invalid partition selected. Please select from one of the following :\n{bc4_partitions}")

    n_nodes = job_data["n_nodes"]
    n_tasks_per_node = job_data["n_tasks_per_node"]
    n_cpus_per_task = job_data["n_cpus_per_task"]
    # This is in GB
    memory_req = job_data["memory_req"]
    job_duration = job_data["job_duration"]

    # TODO - in the future this can set whether we use BC4/CitC etc
    # service = job_data["service"]
    
    # This feels a bit clunky
    if not memory_req.endswith("G"):
        raise ValueError("Memory requirements must be in gigabytes and end with a G i.e. 128G")

    # Create a dated and named filename and path in the user's home dir
    date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    name_date = f"{name}_{date_str}"
    script_filename = f"run_job_{name_date}.sh"

    # Create a JSON file that will hold the job parameters
    json_dict = {}
    json_dict["job_name"] = f"job_{name_date}"
    json_dict["job_data"] = job_data
    json_dict["script_filename"] = script_filename
    json_dict["par"] = job_data["par"]
    json_dict["par_secret"] = job_data["par_secret"]

    json_filename = f"job_data_{name_date}.json"

    # Here we want to write the jobscript and job parameters to file before transferring
    # to the cluster
    with tempfile.TemporaryDirectory() as tmpdir, SSHConnect() as sc:
        jobscript_path = Path(tmpdir).joinpath(script_filename)
        json_path = Path(tmpdir).joinpath(json_filename)

        with open(json_path, "w") as jf:
            json.dump(obj=json_dict, fp=jf, indent=4)

        with open(jobscript_path, 'w') as rsh:
            rsh.write(f"""#!/bin/bash
        #SBATCH --partition={"test"}
        #SBATCH --nodes={n_nodes}
        #SBATCH --ntasks-per-node={n_tasks_per_node}
        #SBATCH --cpus-per-task={n_cpus_per_task}
        #SBATCH --time={job_duration}
        #SBATCH --mem={memory_req}
        {run_command}
            """)

        # Here we'll only copy the files we've created
        # Other input files will be copied from the cloud drive by the  script we're passing
        job_controller = get_datapath(filename="bc4_template.py", directory="job_controllers")

        # TODO - add in controller script here
        files = [jobscript_path, json_path, job_controller]

        sc.connect(username=username, hostname=hostname)
        sc.write_files(files=files, remote_dir="first_job")
        response_list = sc.run_command(commands=f"python bc4_template.py {json_filename} &")

    return response_list
        





    


    
