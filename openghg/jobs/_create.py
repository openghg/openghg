__all__ = ["JobCreator"]

# type: ignore


class JobCreator:
    """Create a job to be run on a HPC cluster

    WIP: Skeleton at the moment, will be filled out for use with multiple HPC
    resources/CitC
    """

    def __init__(self):
        self._jobs = []

    def create_job(self, name, requirements, data):
        """Create a job script

        TODO: Create an actual job script for

        For now just create a script file

        Args:
            name (str): Job name
            requirements (dict): Dictionary containing requested resources
            Example:
                requirements = {"cores": 16, "memory": 128G, "duration": 12h}
            For a job running with 16 cores and requesting 128 GB of memory for 12 hours
            (if duration is required)
        Returns:
            str: Job script as string
        """
        return "Sweet job script"
