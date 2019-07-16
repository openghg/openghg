"""
This script writes the login information (pem key etc.) that is needed
by the identity service to log onto the object store as the
identity admin user account
"""

import json
import sys
import os

from Acquire.Crypto import PrivateKey
from Acquire.ObjectStore import bytes_to_string

## Create a key to encrypt the config
config_key = PrivateKey()
secret_config = {}

## First create the login info to connect to the account

data = {}

# OCID for the user
data["user"] = "ocid1.user.oc1..aaaaaaaas2elik6wbuvvab5qeewdj5dkzs3kyi3w3jqhkgbmjub272oyfmda"

# Fingerprint for the login keyfile
data["fingerprint"] = "72:8b:ab:c6:78:19:f8:6e:ef:cd:2f:7e:95:cb:a0:b7"

# The keyfile itself - we will now read the file and pull it into text
keyfile = sys.argv[1]
data["key_lines"] = open(sys.argv[1],"r").readlines()

# The tenancy in which this user and everything exists!
data["tenancy"] = "ocid1.tenancy.oc1..aaaaaaaaadarqjzv7bxmb3scckyhmqlqyok3dfqenmzyirrbpjv32uyy74ca"

# The passphrase to unlock the key - VERY SECRET!!!
data["pass_phrase"] = sys.argv[2]

# Make sure that this is the correct password...
privkey = PrivateKey.read(sys.argv[1],sys.argv[2])

# The region for this tenancy
data["region"] = "eu-frankfurt-1"

secret_config["LOGIN"] = data

## Now create the bucket info so we know where the bucket is
## that will store all data related to logging into accounts

data = {}
data["compartment"] = "ocid1.compartment.oc1..aaaaaaaawwwf4eocadm6suukafuemsxqfk74frirbmzskptqa4rkrwumtnpa"
data["bucket"] = "hugs_principal"

secret_config["BUCKET"] = data

secret_config["PASSWORD"] = sys.argv[2]

config_data = bytes_to_string(config_key.encrypt(json.dumps(secret_config).encode("utf-8")))
secret_key = json.dumps(config_key.to_data(sys.argv[3]))

os.system("fn config app hugs SECRET_CONFIG '%s'" % config_data)
os.system("fn config app hugs SECRET_KEY '%s'" % secret_key)
