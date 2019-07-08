
import os
import sys

name = sys.argv[1]
passphrase = sys.argv[2]

cmd = "openssl genrsa -out %s.pem -aes128 -passout pass:%s 2048" % (name,passphrase)
os.system(cmd)

cmd = "openssl rsa -pubout -in %s.pem -out %s_public.pem -passin pass:%s" % (name,name,passphrase)
os.system(cmd)

