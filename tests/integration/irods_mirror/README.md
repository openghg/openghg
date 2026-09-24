# Disposable iRODS mirror lab

This lab runs a catalogue provider and a separate consumer server in one iRODS
zone. `originResc` belongs to `provider`; `mirrorResc` belongs to `consumer`.
Their vaults are in separate Docker volumes. The consumer represents a shared
mirror serving many readers, including the case where a laptop hosts a small
experimental mirror server.

The lab creates its own PostgreSQL database, credentials, CA, and certificates.
It does not connect to an existing deployment, copy server credentials, publish
host ports, or install software on the host. Native TLS is required in both
directions, with hostname verification for `provider` and `consumer`.

## Run the checks

Prerequisites: Docker Engine or Docker Desktop with a recent Compose v2, access
to Docker Hub, the iRODS package repository and PyPI, and enough disk space for the
server image and OpenGHG's scientific Python dependencies. The server image uses
Ubuntu 24.04 with iRODS **5.0.2** packages; the client pins Python iRODS Client
**3.3.0**. PostgreSQL uses `postgres:16.15-bookworm`. The iRODS packages
are AMD64, so ARM hosts need Docker's AMD64 emulation.

From the OpenGHG repository root, run:

```sh
docker compose -p openghg-mirror-lab \
  -f tests/integration/irods_mirror/compose.yaml \
  run --build --rm client
```

This initializes the isolated servers, waits for authenticated TLS health checks,
and runs `tests/objectstore/test_irods_mirror.py` inside the client container.
The tests receive the provider and consumer connection files automatically.
They require distinct native resource locations, check a transfer large enough
to exercise native transfer-mode handling, and exercise reader permissions.
They create uniquely named temporary collections and remove those collections.

The iRODS servers remain available for another test run. To run a different test
selection in the same lab, replace the client command:

```sh
docker compose -p openghg-mirror-lab \
  -f tests/integration/irods_mirror/compose.yaml \
  run --rm client pytest -q tests/objectstore/test_irods_mirror.py -k live
```

Remove this project's servers, database, vaults, and generated credentials when
finished:

```sh
docker compose -p openghg-mirror-lab \
  -f tests/integration/irods_mirror/compose.yaml \
  down --volumes --remove-orphans
```

Use a distinct project name for concurrent labs. Do not reuse this project's
volumes for data you intend to retain. Certificates expire after fourteen days;
remove the lab volumes and initialize a fresh lab after expiry.

## What the harness supplies

The client container has the following test configuration:

| Variable | Value |
| --- | --- |
| `IRODS_ENVIRONMENT_FILE` | `/run/irods-client/environment.json` |
| `OPENGHG_IRODS_MIRROR_ENVIRONMENT_FILE` | `/run/irods-client/consumer-environment.json` |
| `OPENGHG_IRODS_TEST_COLLECTION` | `/mirrorZone/home/rods` |
| `OPENGHG_IRODS_SOURCE_RESOURCE` | `originResc` |
| `OPENGHG_IRODS_MIRROR_RESOURCE` | `mirrorResc` |
| `OPENGHG_IRODS_MIRROR_REQUIRE_DISTINCT_HOSTS` | `1` |
| `OPENGHG_IRODS_ADMIN_TESTS` | `1` |

Credential and installation files are generated privately at runtime; they are
not baked into either image or printed by the bootstrap. The administration
credential is available only inside this disposable lab. Role tests create
separate native users rather than treating a client configuration flag as an
access-control boundary.

The consumer is a trusted member of the provider's zone. Its native unattended
installer registers `mirrorResc` as a `unixfilesystem` resource on the consumer
server. Successful replication must therefore create another replica of the
same catalogue object, with a matching data ID and checksum. A second directory
on the provider is not accepted as evidence of a remote server.

`numThreads=-1` is the native iRODS request setting for transfers through control
connections. A value of `0` lets iRODS choose a transfer mode; `1` can still use a
separate transfer portal. The backend's mirror path controls this setting for
replication. This lab leaves both server containers on the same private network;
it does not establish that a site's firewall, VPN, NAT, or SSH route permits all
required communication.

## Inspect a failed startup

Inspect the container status and public server logs:

```sh
docker compose -p openghg-mirror-lab \
  -f tests/integration/irods_mirror/compose.yaml ps -a
docker compose -p openghg-mirror-lab \
  -f tests/integration/irods_mirror/compose.yaml logs provider consumer
```

If the unattended installer fails, it records details in the failed container's
private `/run/irods-bootstrap/setup.log`. Treat that file as potentially sensitive
and inspect it locally; do not upload it as a CI artifact. The bootstrap prints
only the final twenty lines after redacting the generated credentials, so a CI
run has a bounded startup diagnostic. After correcting the image or setup code,
remove the project volumes before a clean run.

The setup uses the template installed by the pinned iRODS package and the native
`setup_irods.py --json_configuration_file` interface. References are the upstream
[iRODS testing environment](https://github.com/irods/irods_testing_environment)
and its [provider/consumer Compose topology](https://github.com/irods/irods_testing_environment/blob/05bd4ab612b9b8e1e076086f75c2344219c62180/projects/ubuntu-24.04/ubuntu-24.04-postgres-16/docker-compose.yml).
Unlike the upstream test orchestrator, this bootstrap does not print unattended
installation input.

This is a development harness, not a production service installer. It runs two
server processes in separate containers on one Docker host. A successful run does
not demonstrate a real laptop connection, independent-host failure tolerance,
offline catalogue operation, or Blue Pebble network reachability.
