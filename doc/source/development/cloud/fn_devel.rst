==============
Fn Development
==============

Fn is an open-source framework for creating a Functions-as-a-Service (FaaS) compute platform. In real terms this means
we make a HTTP POST call to our Fn server, ask it to call a function which then returns a result.

Getting setup
-------------

To run through this tutorial you must have both Docker and Fn.

For Docker installation instructions please visit `the Docker installation instruction <https://docs.docker.com/get-docker/>`__.

To install Fn follow the instructions for your operating system given on `their GitHub repo <https://github.com/fnproject/fn>`__.

Starting Fn
-----------

As we'll be developing functions locally we'll want to start up an instance of ``Fn`` to allow us to run (invoke) the functions
we build. During development setting the log level of ``Fn`` to ``DEBUG`` is highly recommended. This pipes errors that may occur
within the containers to the terminal. Without setting the log level the only error reporting shown is a less helpful
``Error invoking function. status: 502 message: function failed`` message.

So, open a terminal and run (and leave running)

.. code-block:: bash

   fn start --log-level DEBUG

If you don't want extra debug information from each invocation just run

.. code-block:: bash

   fn start

First function
--------------

To call functions within OpenGHG we use a routing function with Fn. This function (link to route.py) routes calls made to
our Fn triggers (more on triggers later). To setup a function we first need to create an app. To do this we use

.. code-block:: bash

   fn init --runtime python openghg_fn

This will create a folder called ``openghg_fn`` with some boilerplate code inside. These can form a template
for creation of your own functions. If we look inside this directory we'll see three files

.. code-block:: bash

    $ cd openghg_fn/
    $ ls
    func.py  func.yaml  requirements.txt

Here ``func.py`` is the function we're going to call, ``func.yaml`` tells Fn how to setup and run the function and ``requirements.txt``
contains the Python requirements and is used when a Docker image is built for the function.

Function code
^^^^^^^^^^^^^

First we'll modify ``func.py`` to contain a simple response to being called. Paste the following into ``func.py``:

.. code-block:: python

    import io
    import json
    import logging
    from typing import Dict, Optional, Union

    from fdk.response import Response


    def route(ctx, data: Optional[Union[io.BytesIO, Dict]] = None) -> Response:
        message = {"message": "Hello from OpenGHG"}
        return_str = json.dumps(message)
        headers = {"Content-Type": "application/json"}

        return Response(ctx, response_data=return_str, headers=headers)

Function parameters
^^^^^^^^^^^^^^^^^^^

Next we'll modify ``func.yaml`` to contain

.. code-block:: yaml

    schema_version: 20180708
    name: openghg_fn
    version: 0.0.1
    runtime: python
    entrypoint: /python/bin/fdk /function/func.py route
    # Memory limit for function in MB
    memory: 256

Here note the change of ``handler`` to ``route`` for the entrypoint.

Seeing as we're just using a bare Fn function here we can leave ``requirements.txt`` as it is with our only requirement being ``fdk``.
As each function needs to be part of an app we create an app called ``openghg``.

.. code-block:: bash

    fn create app openghg

We're now ready to deploy our function and call it.

Deployment
^^^^^^^^^^

To deploy the app we can run

.. code-block:: bash

    fn --verbose deploy --local

This tells Fn give us verbose output and ``--local`` tells it not to push our Docker image to DockerHub. After you run this command
you'll see a lot of output as Fn builds a Docker. Hopefully at the end of the output you'll have something similar to

.. code-block:: bash

    Successfully built 22ed08c1f99e
    Successfully tagged openghg_fn:0.0.2

    Updating function openghg_fn using image openghg_fn:0.0.2...
    Successfully created function: openghg_fn with openghg_fn:0.0.2

Don't worry if some values are slightly different here.

Call
^^^^

You should then be able to invoke / call the function using

.. code-block:: bash

    [user@computer openghg_fn]$ fn invoke openghg openghg_fn
    {"message": "Hello from OpenGHG"}

We can also use ``curl`` to trigger the function. If we do

.. code-block:: bash

    [user@computer openghg_fn]$ fn inspect function openghg openghg_fn
    {
        "annotations": {
            "fnproject.io/fn/invokeEndpoint": "http://localhost:8080/invoke/01ES9D6TA5NG8G00GZJ0000009"
        },
        "app_id": "01ES9D6T23NG8G00GZJ0000008",
        "created_at": "2020-12-11T17:22:35.461Z",
        "id": "01ES9D6TA5NG8G00GZJ0000009",
        "idle_timeout": 30,
        "image": "openghg:0.0.82",
        "memory": 2048,
        "name": "openghg",
        "timeout": 30,
        "updated_at": "2021-06-08T13:26:48.066Z"
    }


We can see that there is an invocation endpoint at ``http://localhost:8080/invoke/01ES9D6TA5NG8G00GZJ0000009``, using curl we can
call the function like so

.. code-block:: bash

    [user@computer openghg_fn]$ curl -X POST http://localhost:8080/invoke/01ES9D6TA5NG8G00GZJ0000009
    {"message": "Hello from OpenGHG"}

Note that your invocation endpoint may differ slightly from the one shown above.

Dockerise
---------

As our functions will be more complex than the example given above we need to create our own custom Docker image.
To create our own Docker image for the function we've created above create a ``Dockerfile`` in the ``openghg_fn`` folder
that contains the following:

.. code-block:: dockerfile

    FROM fnproject/python:3.8.5

    ADD requirements.txt func.py function/
    WORKDIR /function

    RUN pip3 install pip==20.2.4 wheel setuptools
    RUN pip3 install --target /python/ -r requirements.txt
    RUN rm -rf requirements.txt

    ENV PYTHONPATH=/python

    ENTRYPOINT ["/python/bin/fdk", "/function/func.py", "route"]

Here we've installed a specific ``pip`` version 20.2.4 as this was the last version before the new resolver was introduced.

After creating our ``Dockerfile`` we must also update ``func.yaml`` to create tell Fn that we're now using our own customer Docker container

.. code-block:: yaml

    schema_version: 20180708
    name: openghg_fn
    version: 0.0.4
    runtime: docker
    triggers:
    - name: route
    type: http
    source: /openghg_fn

We can then tell Fn to deploy the image again. This will build the container using our custom Dockerfile.

.. code-block:: bash

    fn --verbose deploy --local

Hopefully at the end of the build you'll see something like:

.. code-block:: bash

    Updating function openghg_fn using image openghg_fn:0.0.4...
    Successfully created trigger: route
    Trigger Endpoint: http://localhost:8080/t/openghg/openghg_fn

We now have a much cleaner endpoint we can use to trigger the function. Using ``curl`` again to trigger the function. Note that
trigger/invoke/call are all used interchangeably here.

.. code-block:: bash

    [user@computer openghg_fn]$ curl -X POST http://localhost:8080/t/openghg/openghg_fn
    {"message": "Hello from OpenGHG"}

Now we have an understanding of how Fn works and how to create functions and call them we will cover the functions available in OpenGHG.

OpenGHG functions
-----------------

With OpenGHG we use a single routing function to route calls to a number of separate functions. This routing function can be found
in ``services/route.py``. To use this routing function we first need to setup a Docker container within which we can perform the
computation and return the data to the caller. As OpenGHG requires a number of packages we use a two step build process.
First we create a base image, called ``openghg-base`` which contains all the requirements for OpenGHG. We then use this base image
to create a second image, called ``openghg-complete``, into which we copy our OpenGHG library code and services/function code.

Base image
^^^^^^^^^^

First we'll look at the base image which can be found in ``docker/base_image``. This folder contains a Python script
that makes building the image easier and a Dockerfile.

.. literalinclude:: ../../../../docker/base_image/Dockerfile
    :language: dockerfile

This ``Dockerfile`` is very similar to the one shown above. Some differences are that we copy an extra requirements file into the image
and install ``git`` to allow ``pip`` to install Acquire from GitHub. Another difference is that we use two build stages. The first using
the ``fnproject/python:3.8.5-dev as build-stage``. After cloning Acquire and installing all the packages into ``/python`` we start
with a fresh image and copy only the contents of ``/python`` into this image. This helps limit the size of the image.

To build this image run

.. code-block:: bash

    python build.py

This will build a Docker image with the tag ``openghg/openghg-base:latest``. To see the available options when building the image
run the command above with ``-h``.

Complete image
^^^^^^^^^^^^^^

Now we've built the base image we can build the complete image containing the OpenGHG library and services code. The files to build this image
can be found in ``docker/``. It contains ``func.yaml`` which tells Fn how to run our function. The ``Dockerfile`` (shown below) uses
the ``openghg-base`` image we build in the previous step, adding ``route.py`` to the ``/function`` folder and then copying the OpenGHG
code into ``/python``. We also copy the services code which form the functions that calls are routed to by ``route.py``.

.. literalinclude:: ../../../../docker/Dockerfile
    :language: dockerfile

We have also modified ``func.yaml`` to increase the amount of memory available to this function to 2048 MB / 2 GB. If you notice functions failing unexpectedly
it may be worth trying changing this value.

.. literalinclude:: ../../../docker/func.yaml
    :language: yaml

To build this image we use the ``build_deploy.py`` Python script.

.. code-block:: bash

    [user@computer docker]$ python build_deploy.py -h
    usage: build_deploy.py [-h] [--tag TAG] [--push] [--build] [--deploy] [--build-base]

    Build the base Docker image and optionally push to DockerHub

    optional arguments:
    -h, --help    show this help message and exit
    --tag TAG     tag name/number, examples: 1.0 or latest. Not full tag name such as openghg/openghg-complete:latest. Default: latest
    --push        push the image to DockerHub
    --build       build the docker image. Disables Fn deploy.
    --deploy      buid image and deploy the Fn functions
    --build-base  build the base docker image before building the complete image

This script takes care of building the base image as well if you want it to. To build both the base and complete image and deploy the
functions to Fn run

.. code-block:: bash

    python build.py --build-base

If you have the base image built and have only made changes to the OpenGHG code you can just run

.. code-block:: bash

    python build.py

**Note** - if you've made changes to either ``requirements.txt`` or ``requirements-server.txt`` you'll need to do a rebuild of the base
image. This ensures all dependencies are installed in the base image.

Test a function
---------------

Say we want to test a function such as the ``testconnection`` function that is a part of OpenGHG. This simple function returns a simple
string of the timestamp at which the function was called to the user.

As above we inspect the funtion and find its endpoint.

.. code-block:: bash

    [user@computer openghg_fn]$ fn inspect function openghg openghg
    {
        "annotations": {
            "fnproject.io/fn/invokeEndpoint": "http://localhost:8080/invoke/01ES9D6TA5NG8G00GZJ0000009"
        },
        "app_id": "01ES9D6T23NG8G00GZJ0000008",
        "created_at": "2020-12-11T17:22:35.461Z",
        "id": "01ES9D6TA5NG8G00GZJ0000009",
        "idle_timeout": 30,
        "image": "openghg:0.0.82",
        "memory": 2048,
        "name": "openghg",
        "timeout": 30,
        "updated_at": "2021-06-08T13:26:48.066Z"
    }

We can then call the function using

.. code-block:: bash

    curl -X POST -d '{"function" : "testconnection", "args": {}}' http://localhost:8080/invoke/01ES9D6TA5NG8G00GZJ0000009

.. note::
    Your endpoint URL will differ from the one above.

And we should recieve a response such as

.. code-block:: bash

    {'results': 'Function run at 2021-06-08 13:34:17.095579+00:00'}

Now we know our Dockerised function can be called and works correctly.

Calling from OpenGHG
--------------------

For information on how we've setup calling functions from OpenGHG please see the :doc:`fn_usage` section of the documentation.
