import getpass
import paramiko
import os
import socket
import sys
import traceback
from pathlib import Path

# See this for tests
# https://pypi.org/project/mock-ssh-server/

# __all__ == ["SSHConnect"]

class SSHConnect:
    """ Use Paramiko to connect to an SSH server

        This class can be used as a context manager
    """
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __init__(self):
        self._host_keys = {}
        self._port = 22
        self._hostkey = None
        # This will be the paramiko.SSHClient object
        self._client = None

    def close(self):
        """ Calls close on the paramiko SSHClient instance used
            the SSH connection

            Returns:
                None
        """
        try:
            self._client.close()
        except AttributeError:
            return

    def connect(self, username, hostname):
        """ Use Paramiko to connect the hostname

            Args:
                user (str): Username for login
                hostname (str): Hostname of server. This can also be an IP address
                key_path (Path or str): Path to private key
            Returns:
                None
        """
        self._client = paramiko.SSHClient()
        
        self._client.load_system_host_keys()
        self._client.connect(hostname=hostname, port=22, username=username)

    def run_command(self, commands):
        """ Run commands on the remote server

            Args:
                commands (str, list): Command(s) to be run on the remote server
            Returns:
                list: List of tuples of stdin, stdout and stderr for each command
        """
        responses = {}

        if not isinstance(commands, list):
            commands = [commands]

        for c in commands:
            stdin, stdout, stderr = self._client.exec_command(c)
            
            # Get bytes from the objects and convert to str
            stdout = stdout.read().decode("utf-8")
            stderr = stderr.read().decode("utf-8")

            responses[c] = {"stdout": stdout, "stderr": stderr}

        return responses


    def write_files(self, files, remote_dir=None):
        """ Write the job script to the remote server

            Args:
                files (list): List of paths of files to transfer
                path (str, default=None): Path to write script, if not passed the script
                will be written to the user's home directory
            Returns:
                None
        """
        sftp = self._client.open_sftp()

        if not isinstance(files, list):
            files = [files]

        # Conver to pathlib.Path objects for easier handling
        files = [Path(f) for f in files]

        if remote_dir is not None:
            remote_dir = Path(remote_dir)
            try:
                sftp.mkdir(str(remote_dir))
            except IOError:
                # This probably means the directory already exists
                # TODO - better way of handling this error?
                pass
                
        for filepath in files:
            # Here we only want the filename
            if remote_dir is not None:
                remote_path = remote_dir.joinpath(filepath.name).name
            else:
                remote_path = filepath.name

            r = sftp.put(localpath=filepath, remotepath=remote_path)
