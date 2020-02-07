import getpass
import paramiko
import os
import socket
import sys
import traceback

# See this for tests
# https://pypi.org/project/mock-ssh-server/

__all__ == ["SSHConnect"]

class SSHConnect:
    """ Use Paramiko to connect to an SSH server

        Not sure if this needs to be a class

    """

    def __init__(self):
        self._host_keys = {}
        self._use_GSSAPI = False  # enable GSS-API / SSPI authentication
        self._DoGSSAPIKeyExchange = False
        self._port = 22
        self._hostkey = None

    def connect(self, username, hostname):
        """ Use Paramiko to connoect the hostname

            Args:
                user (str): Username for login
                hostname (str): Hostname of server. This can also be an IP address
                key_path (Path or str): Path to private key
        """
        with paramiko.SSHClient() as client:
            client.load_system_host_keys()
            # This automatically searches for keys to use, otherwise we can pass in a key_filename
            client.connect(hostname=hostname, port=22, username=username)
            
            # Using these the folders must exist already
            r = sftp.put("demo_sftp.py", "demo_sftp_folder/demo_sftp.py")
            s = sftp.get("demo_sftp_folder/README", "README_demo_sftp")

        # with paramiko.Transport((hostname, self._port)) as t:
        #     username = "sshtest"
        #     password = "Ilovesshtesting123"

        #     t.connect(self._hostkey, username, password, gss_host=socket.getfqdn(hostname), gss_auth=self._use_GSSAPI, gss_kex=self._DoGSSAPIKeyExchange)

        #     t.connect()
        #     sftp = paramiko.SFTPClient.from_transport(t)

            # Use SFTP to copy files
            # SSH to copy

            # try:
            #     sftp.mkdir("demo_sftp_folder")
            # except IOError:
            #     print("(assuming demo_sftp_folder/ already exists)")
            # with sftp.open("demo_sftp_folder/README", "w") as f:
            #     f.write("This was created by demo_sftp.py.\n")
            # with open("demo_sftp.py", "r") as f:
            #     data = f.read()
            # sftp.open("demo_sftp_folder/demo_sftp.py", "w").write(data)
            # # print("created demo_sftp_folder/ on the server")

            # # Using these the folders must exist already
            # sftp.put("demo_sftp.py", "demo_sftp_folder/demo_sftp.py")
            # sftp.get("demo_sftp_folder/README", "README_demo_sftp")


# if __name__ == "__main__":
#     c = SSHConnect()

#     username = "sshtest"
#     # hostname = "bc4login.acrc.bris.ac.uk"
#     hostname = "127.0.0.1"
#     keypath = "/home/gar/.ssh/id_rsa"

#     c.connect(username=username, hostname=hostname)
