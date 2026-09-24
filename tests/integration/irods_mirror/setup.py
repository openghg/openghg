"""Configure a disposable iRODS 5 provider/consumer lab without printing secrets.

This is container bootstrap code, never a launcher for an existing deployment.
The source of the server defaults is the template shipped in the pinned package.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

SECRETS = Path("/lab-secrets")
IRODS = Path("/var/lib/irods")
CONFIG = Path("/etc/irods")
ZONE = "mirrorZone"
RESOURCES = {"provider": "originResc", "consumer": "mirrorResc"}


def private_write(path: Path, value: str) -> None:
    """Write a private runtime file; all callers use isolated container paths."""
    path.write_text(value)
    path.chmod(0o600)


def quiet(*args: str) -> None:
    """Run setup utilities without placing their generated configuration in logs."""
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def initialize() -> None:
    """Create throwaway zone credentials and a CA with two hostname certificates."""
    SECRETS.mkdir(mode=0o700, parents=True, exist_ok=True)
    SECRETS.chmod(0o700)
    if (SECRETS / "ready").exists():
        return
    for name in ("database-password", "admin-password"):
        private_write(SECRETS / name, secrets.token_urlsafe(32))
    private_write(SECRETS / "zone-key", secrets.token_hex(24))
    private_write(SECRETS / "negotiation-key", secrets.token_hex(16))
    quiet(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-days",
        "14",
        "-subj",
        "/CN=OpenGHG disposable mirror lab CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-keyout",
        str(SECRETS / "ca.key"),
        "-out",
        str(SECRETS / "ca.pem"),
    )
    quiet(
        "openssl",
        "genpkey",
        "-genparam",
        "-algorithm",
        "DH",
        "-pkeyopt",
        "group:ffdhe2048",
        "-out",
        str(SECRETS / "dhparams.pem"),
    )
    for host in RESOURCES:
        quiet(
            "openssl",
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-sha256",
            "-subj",
            f"/CN={host}",
            "-keyout",
            str(SECRETS / f"{host}.key"),
            "-out",
            str(SECRETS / f"{host}.csr"),
        )
        extensions = SECRETS / f"{host}.extensions"
        private_write(
            extensions,
            f"subjectAltName=DNS:{host}\nextendedKeyUsage=serverAuth\n"
            "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n",
        )
        quiet(
            "openssl",
            "x509",
            "-req",
            "-sha256",
            "-days",
            "14",
            "-in",
            str(SECRETS / f"{host}.csr"),
            "-CA",
            str(SECRETS / "ca.pem"),
            "-CAkey",
            str(SECRETS / "ca.key"),
            "-CAcreateserial",
            "-extfile",
            str(extensions),
            "-out",
            str(SECRETS / f"{host}.pem"),
        )
    for path in SECRETS.iterdir():
        path.chmod(0o600)
    private_write(SECRETS / "ready", "Generated for this disposable Compose project.\n")


def environment(host: str, ca: Path, auth: Path | None = None) -> dict:
    """Return client settings with required TLS and hostname verification."""
    result = {
        "irods_host": host,
        "irods_port": 1247,
        "irods_user_name": "rods",
        "irods_zone_name": ZONE,
        "irods_authentication_scheme": "native",
        "irods_home": f"/{ZONE}/home/rods",
        "irods_cwd": f"/{ZONE}/home/rods",
        "irods_default_resource": RESOURCES[host],
        "irods_default_hash_scheme": "SHA256",
        "irods_match_hash_policy": "compatible",
        "irods_connection_pool_refresh_time_in_seconds": 300,
        "irods_default_number_of_transfer_threads": 4,
        "irods_maximum_size_for_single_buffer_in_megabytes": 32,
        "irods_transfer_buffer_size_for_parallel_transfer_in_megabytes": 4,
        "irods_client_server_negotiation": "request_server_negotiation",
        "irods_client_server_policy": "CS_NEG_REQUIRE",
        "irods_ssl_ca_certificate_file": str(ca),
        "irods_ssl_verify_server": "hostname",
        "irods_encryption_algorithm": "AES-256-CBC",
        "irods_encryption_key_size": 32,
        "irods_encryption_num_hash_rounds": 16,
        "irods_encryption_salt_size": 8,
        "schema_name": "service_account_environment",
        "schema_version": "v5",
    }
    if auth is not None:
        result["irods_authentication_file"] = str(auth)
    return result


def server(role: str) -> None:
    """Set up one server using the stock unattended installer, then run it in front."""
    runstate = Path("/run/irods")
    runstate.mkdir(mode=0o755, parents=True, exist_ok=True)
    shutil.chown(runstate, user="irods", group="irods")
    configured = IRODS / ".openghg-mirror-lab-configured"
    if not configured.exists():
        tls = CONFIG / "tls"
        tls.mkdir(parents=True, exist_ok=True)
        for source, name in (
            ("ca.pem", "ca.pem"),
            ("dhparams.pem", "dhparams.pem"),
            (f"{role}.pem", "server.pem"),
            (f"{role}.key", "server.key"),
        ):
            private_write(tls / name, (SECRETS / source).read_text())
        rules = (IRODS / "packaging/core.re.template").read_text()
        private_write(CONFIG / "core.re", rules.replace('"CS_NEG_REFUSE"', '"CS_NEG_REQUIRE"'))
        configuration = json.loads((IRODS / "packaging/server_config.json.template").read_text())
        configuration.update(
            {
                "host": role,
                "catalog_service_role": role,
                "catalog_provider_hosts": ["provider"],
                "zone_name": ZONE,
                "zone_port": 1247,
                "zone_user": "rods",
                "zone_key": (SECRETS / "zone-key").read_text(),
                "negotiation_key": (SECRETS / "negotiation-key").read_text(),
                "default_resource_name": RESOURCES[role],
                "server_port_range_start": 20000,
                "server_port_range_end": 20199,
                "client_server_policy": "CS_NEG_REQUIRE",
                "tls_server": {
                    "certificate_chain_file": str(tls / "server.pem"),
                    "certificate_key_file": str(tls / "server.key"),
                    "dh_params_file": str(tls / "dhparams.pem"),
                },
                "tls_client": {"ca_certificate_file": str(tls / "ca.pem"), "verify_server": "hostname"},
            }
        )
        if role == "provider":
            configuration["plugin_configuration"]["database"] = {
                "technology": "postgres",
                "host": "catalog",
                "name": "ICAT",
                "port": 5432,
                "username": "irods",
                "password": (SECRETS / "database-password").read_text(),
                "odbc_driver": "PostgreSQL ANSI",
            }
        install = {
            "host_system_information": {
                "service_account_user_name": "irods",
                "service_account_group_name": "irods",
            },
            "admin_password": (SECRETS / "admin-password").read_text(),
            "default_resource_name": RESOURCES[role],
            "default_resource_directory": str(IRODS / "Vault"),
            "service_account_environment": environment(role, tls / "ca.pem"),
            "server_config": configuration,
        }
        runtime = Path("/run/irods-bootstrap")
        runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
        install_path = runtime / "install.json"
        private_write(install_path, json.dumps(install))
        log_path = runtime / "setup.log"
        private_write(log_path, "")
        try:
            with log_path.open("a") as log:
                subprocess.run(
                    [
                        "python3",
                        str(IRODS / "scripts/setup_irods.py"),
                        "--json_configuration_file",
                        str(install_path),
                    ],
                    check=True,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
        except subprocess.CalledProcessError:
            # CI can diagnose startup without publishing the full installation
            # input. Redact every generated credential before taking the tail.
            diagnostic = log_path.read_text(errors="replace")
            for name in ("database-password", "admin-password", "zone-key", "negotiation-key"):
                diagnostic = diagnostic.replace((SECRETS / name).read_text(), "<redacted>")
            print("iRODS setup diagnostic (generated credentials redacted):", file=sys.stderr)
            print("\n".join(diagnostic.splitlines()[-20:]), file=sys.stderr)
            raise SystemExit(
                "iRODS setup failed; inspect the private /run/irods-bootstrap/setup.log inside this container."
            ) from None
        finally:
            install_path.unlink(missing_ok=True)
        # The installer starts a daemon. Stop it before handing the foreground
        # process to the container runtime so normal container shutdown works.
        quiet(
            "runuser",
            "-u",
            "irods",
            "--",
            "python3",
            "-c",
            "import sys; sys.path.insert(0, '/var/lib/irods/scripts'); "
            "from irods.controller import IrodsController; IrodsController().stop()",
        )
        private_write(configured, "Configured from this project's generated credentials.\n")
    os.execvp("runuser", ["runuser", "-u", "irods", "--", "irodsServer", "--stdout"])


def client(command: list[str]) -> None:
    """Create private native credential files and execute the integration tests."""
    from irods.client_init import write_native_irodsA_file

    runtime = Path("/run/irods-client")
    runtime.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copyfile(SECRETS / "ca.pem", runtime / "ca.pem")
    for host, filename in (("provider", "environment.json"), ("consumer", "consumer-environment.json")):
        path = runtime / filename
        auth = runtime / f"{host}.irodsA"
        private_write(path, json.dumps(environment(host, runtime / "ca.pem", auth)))
        write_native_irodsA_file((SECRETS / "admin-password").read_text(), irods_env_file=str(path))
    os.execvp(command[0], command)


if __name__ == "__main__":
    os.umask(0o077)
    action = sys.argv[1]
    if action == "initialize":
        initialize()
    elif action in RESOURCES:
        server(action)
    elif action == "client":
        client(sys.argv[2:])
    else:
        raise SystemExit(f"Unknown lab action: {action}")
