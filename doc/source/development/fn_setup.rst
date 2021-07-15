=========
Fn setup
=========

This guide will help you get a basic Fn server setup and ready to be given functions for invocation. In future tutorials
we will cover getting Fn setup using Kubernetes.

Server
------

In this tutorial we'll be using a virtual machine cloud instance on the Oracle cloud, but any cloud provider
will work. Although we'll use a CentOS 8 image on our VM any Docker capable image should work, though setup steps will difference
from those given here.


Installing Docker
-----------------

We'll install Docker from their repos for CentOS. For the complete
installation instructions please see the `Docker documentation <https://docs.docker.com/engine/install/centos/>`_
but we will summarise the steps below.

First set up the repository

.. code-block:: bash

    sudo yum install -y yum-utils

    sudo yum-config-manager --add-repo \
        https://download.docker.com/linux/centos/docker-ce.repo

Then we can install Docker Engine and containerd

.. code-block:: bash

    sudo yum install -y docker-ce docker-ce-cli containerd.io

Note that you may have to accept the GPG key, please check that it matches the one given in the
`installation instructions <https://docs.docker.com/engine/install/centos/>`_.

Next we must start the docker services

.. code-block:: bash

    sudo systemctl start docker

Then check Docker is working correctly

.. code-block:: bash

    sudo docker run hello-world

As Fn does not currently work with rootless docker we need to add our current user to the ``docker`` group.
Note that this does come with security implications which 
are `outlined here <https://docs.docker.com/engine/security/#docker-daemon-attack-surface>`.

.. code-block:: bash

    sudo usermod -aG docker <username>

Install Fn
----------

We can now install Fn

.. code-block:: bash

    curl -LSs https://raw.githubusercontent.com/fnproject/cli/master/install | sh

Then start Fn to make sure everything runs correctly. On first start some images will be downloaded.

.. code-block:: bash

    fn start

Setup NGINX proxy
-----------------

So that we're not exposing Fn directly to the internet we'll use nginx as a reverse-proxy. First install nginx
using

.. code-block:: bash

    sudo yum -y install nginx

Next we create a server config file for our reverse proxy in ``/etc/nginx/conf.d/fn_proxy.conf``.

.. code-block:: nginx
    :linenos:

        server {
            listen 80 default_server;
            listen [::]:80 default_server;
            server_name fn.openghg.org;

            location / {}

            location /t {
                    proxy_pass http://127.0.0.1:8080/t/openghg;
                    proxy_set_header Host $host;
                    proxy_set_header X-Real-IP $remote_addr;
                    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                    proxy_set_header X-Forwarded-Proto https;
            }
        }


For Acquire we want

.. code-block:: nginx
    :linenos:

        server {
            listen 80 default_server;
            listen [::]:80 default_server;
            server_name acquire.openghg.org;

            location / {
            }

            location /t {
                proxy_pass http://127.0.0.1:8080;
                proxy_set_header Host $host;
                proxy_set_header X-Real-IP $remote_addr;
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                proxy_set_header X-Forwarded-Proto https;
            }
        }
   

Then we want to disable the default config by setting ``/etc/nginx/nginx.conf``.
It might be worth copying up your default ``nginx.conf`` to ``nginx.conf.bak`` before editing
for easy roll-back and comparison.

.. code-block:: nginx
    :linenos:

        user nginx;
        worker_processes auto;
        error_log /var/log/nginx/error.log;
        pid /run/nginx.pid;

        include /usr/share/nginx/modules/*.conf;

        events {
            worker_connections 1024;
        }

        http {
            log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                            '$status $body_bytes_sent "$http_referer" '
                            '"$http_user_agent" "$http_x_forwarded_for"';

            access_log  /var/log/nginx/access.log  main;

            sendfile            on;
            tcp_nopush          on;
            tcp_nodelay         on;
            keepalive_timeout   65;
            types_hash_max_size 2048;

            include             /etc/nginx/mime.types;
            default_type        application/octet-stream;
            include /etc/nginx/conf.d/*.conf;
        }

Then we can check that our configuration setup is valid by doing

.. code-block:: bash

    sudo nginx -t

Then we start and enable nginx

.. code-block:: bash

    sudo systemctl start nginx
    sudo systemctl enable nginx

Setup Firewall Rules
--------------------

First we enable and then start the ``firewalld``

.. code-block:: bash

    sudo systemctl enable firewalld
    sudo systemctl start firewalld

To allow access from the outside world we need to setup rules to allow ``ssh``, ``http`` and ``https`` traffic.

.. code-block:: bash

    sudo firewall-cmd --zone=public --add-service=ssh
    sudo firewall-cmd --zone=public --add-service=ssh --permanent 
    sudo firewall-cmd --zone=public --add-service=https
    sudo firewall-cmd --zone=public --add-service=https --permanent 
    sudo firewall-cmd --zone=public --add-service=http
    sudo firewall-cmd --zone=public --add-service=http --permanent 

Note that we perform the command and then the same command again with the ``--permanent`` argument to add
the rule first to the current session and then to the permanent rule-set.

We also need to tell SELinux to allow HTTP worker_connections

.. code-block:: bash

    sudo setsebool -P httpd_can_network_connect 1

Deploy Fn Functions
-------------------

We now want to deploy our functions. We'll first need to make sure we've got ``git`` and ``python`` installed.

.. code-block:: bash

    sudo yum install python38 git

Next clone the ``openghg`` repository

.. code-block:: bash

    git clone https://github.com/openghg/openghg.git

Then move into the ``openghg/docker`` folder and run

.. code-block:: bash

    python3 build_deploy.py --build-base

The ``--build-base`` argument tells the build script to build the base image. In subsequent deployments we won't need to run this
step unless our dependencies change.

