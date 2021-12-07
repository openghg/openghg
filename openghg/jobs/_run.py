import json
import tempfile
from datetime import datetime
from pathlib import Path

from openghg.jobs import SSHConnect
from openghg.util import get_datapath

# type: ignore


def run_job(username, hostname, password, job_data, known_host=False):
    """Set a job to run on a HPC service

    Args:
        username (str): Username for HPC cluster
        hostname (str): Hostname of HPC cluster
        password (str): Password to unlock private key used to access cluster
        job_data (dict): Data to run job
    Returns:
        dict: Dictionary of responses to commands executing when running the job
    """
    # Maybe this can be passed in / read from JSON depending on the service selected
    bc4_partitions = [
        "cpu_test",
        "dcv",
        "gpu",
        "gpu_veryshort",
        "hmem",
        "serial",
        "test",
        "veryshort",
    ]

    # These are used to write the Slurm script
    # This is a WIP, I might be reinventing the wheel here, check before doing any more
    name = job_data["name"]
    job_sched_command = job_data["job_sched_command"]
    partition = job_data["partition"]

    if partition not in bc4_partitions:
        raise ValueError(
            f"Invalid partition selected. Please select from one of the following :\n{bc4_partitions}"
        )

    n_nodes = job_data["n_nodes"]
    n_tasks_per_node = job_data["n_tasks_per_node"]
    n_cpus_per_task = job_data["n_cpus_per_task"]
    # This shuold be in GB
    memory_req = job_data["memory_req"]

    if not memory_req.endswith("G"):
        raise ValueError("Memory requirements must be in gigabytes and end with a G i.e. 128G")

    job_duration = job_data["job_duration"]

    # Create a dated and named filename and path in the user's home dir
    date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    name_date = f"{name}_{date_str}"
    # Name for the Slurm job script
    script_filename = f"run_job_{name_date}.sh"

    job_name = f"job_{name_date}"

    # Create a JSON file that will hold the job parameters
    json_dict = {}
    json_dict["job_name"] = job_name
    json_dict["job_data"] = job_data
    json_dict["script_filename"] = script_filename
    json_dict["par"] = job_data["par"]
    json_dict["par_secret"] = job_data["par_secret"]
    json_dict["run_command"] = job_data["run_command"]

    try:
        json_dict["compilation_command"] = job_data["compilation_command"]
    except KeyError:
        pass

    # Name of file to write JSON data to for transfer to server
    json_filename = f"job_data_{name_date}.json"

    # Here we want to write the jobscript and job parameters to file before transferring
    # to the cluster
    with tempfile.TemporaryDirectory() as tmpdir, SSHConnect() as sc:
        jobscript_path = Path(tmpdir).joinpath(script_filename)
        json_path = Path(tmpdir).joinpath(json_filename)

        with open(json_path, "w") as jf:
            json.dump(obj=json_dict, fp=jf, indent=4)

        with open(jobscript_path, "w") as rsh:
            rsh.write(
                f"""#!/bin/bash
        #SBATCH --partition={partition}
        #SBATCH --nodes={n_nodes}
        #SBATCH --ntasks-per-node={n_tasks_per_node}
        #SBATCH --cpus-per-task={n_cpus_per_task}
        #SBATCH --time={job_duration}
        #SBATCH --mem={memory_req}
        {job_sched_command}
            """
            )

        # Here we'll only copy the files we've created
        # Other input files will be copied from the cloud drive by the  script we're passing
        job_controller_path = get_datapath(filename="bc4_template.py", directory="job_controllers")

        files = [jobscript_path, json_path, job_controller_path]

        # TODO - I feel this shouldn't be hardwired, it probably won't change in the Docker image, but ?
        keypath = "/home/fnuser/.ssh/runner_key"

        sc.connect(
            username=username,
            hostname=hostname,
            keypath=keypath,
            password=password,
            known_host=known_host,
        )
        # sc.write_files(files=files, remote_dir="first_job")
        sc.write_files(files=files, remote_dir=job_name)

        response_list = sc.run_command(commands=f"cd {job_name}; python3 bc4_template.py {json_filename} &")

    return response_list
