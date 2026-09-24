# iRODS package installation follows the upstream Ubuntu 24.04 testing images.
FROM ubuntu:24.04 AS server
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
ARG DEBIAN_FRONTEND=noninteractive
ARG IRODS_VERSION=5.0.2-0~noble
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg openssl python3 python3-venv procps \
        python3-distro python3-jsonschema python3-psutil python3-requests \
        rsyslog unixodbc odbc-postgresql postgresql-client-16 \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://packages.irods.org/irods-signing-key.asc \
        | gpg --dearmor -o /etc/apt/keyrings/irods.gpg \
    && echo 'deb [signed-by=/etc/apt/keyrings/irods.gpg arch=amd64] https://packages.irods.org/apt/ noble main' \
        > /etc/apt/sources.list.d/irods.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        irods-runtime=${IRODS_VERSION} irods-server=${IRODS_VERSION} \
        irods-icommands=${IRODS_VERSION} irods-database-plugin-postgres=${IRODS_VERSION} \
    && rm -rf /var/lib/apt/lists/*
RUN groupadd --system --gid 2000 irods \
    && useradd --system --uid 2000 --gid irods --home-dir /var/lib/irods \
        --no-create-home --shell /bin/bash irods
COPY tests/integration/irods_mirror/setup.py /lab/setup.py
ENTRYPOINT ["python3", "/lab/setup.py"]

FROM server AS client
COPY . /workspace
RUN python3 -m venv /opt/openghg-env \
    && /opt/openghg-env/bin/pip install --no-cache-dir '/workspace[irods]' \
        'python-irodsclient==3.3.0' 'pytest==7.0.0' 'pytest-mock==3.15.1'
ENV PATH="/opt/openghg-env/bin:${PATH}"
WORKDIR /workspace
ENTRYPOINT ["python", "/lab/setup.py", "client"]
CMD ["pytest", "-q", "tests/objectstore/test_irods_mirror.py"]
