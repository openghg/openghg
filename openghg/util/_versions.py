"""Utility functions for printing version information.

This code was slightly modified from the xarray version

See https://github.com/pydata/xarray/blob/main/xarray/util/print_versions.py

Thank you xarray devs.
"""
import importlib
import locale
import os
import platform
import struct
import subprocess
import sys
from typing import List, IO, Union, Tuple


def get_sys_info() -> List:
    """Returns system information as a list

    Returns:
        list: List of system information
    """
    blob: List[Tuple] = []

    # get full commit hash
    commit: Union[str, bytes, None] = None
    if os.path.isdir(".git") and os.path.isdir("openghg"):
        try:
            pipe = subprocess.Popen(
                'git log --format="%H" -n 1'.split(" "),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            so, _ = pipe.communicate()
        except Exception:
            pass
        else:
            if pipe.returncode == 0:
                commit = so
                try:
                    commit = so.decode("utf-8")
                except ValueError:
                    pass

                commit = commit.strip().strip('"')  # type: ignore

    blob.append(("commit", commit))

    try:
        (sysname, _, release, _, machine, processor) = platform.uname()
        blob.extend(
            [
                ("python", sys.version),
                ("python-bits", struct.calcsize("P") * 8),
                ("OS", f"{sysname}"),
                ("OS-release", f"{release}"),
                # ("Version", f"{version}"),
                ("machine", f"{machine}"),
                ("processor", f"{processor}"),
                ("byteorder", f"{sys.byteorder}"),
                ("LC_ALL", f'{os.environ.get("LC_ALL", "None")}'),
                ("LANG", f'{os.environ.get("LANG", "None")}'),
                ("LOCALE", f"{locale.getlocale()}"),
            ]
        )
    except Exception:
        pass

    return blob


def netcdf_and_hdf5_versions() -> List:
    """Returns the versions of NetCDF and HDF5 libraries installed.

    Returns:
        list: List of versions
    """
    libhdf5_version = None
    libnetcdf_version = None
    try:
        import netCDF4

        libhdf5_version = netCDF4.__hdf5libversion__
        libnetcdf_version = netCDF4.__netcdf4libversion__
    except ImportError:
        pass

    return [("libhdf5", libhdf5_version), ("libnetcdf", libnetcdf_version)]


def show_versions(file: IO = sys.stdout) -> None:
    """print the versions of xarray and its dependencies

    Args:
        file : file-like, optional
            print to the given file-like object. Defaults to sys.stdout.
    """
    sys_info = get_sys_info()

    try:
        sys_info.extend(netcdf_and_hdf5_versions())
    except Exception as e:
        print(f"Error collecting netcdf / hdf5 version: {e}")

    deps = [
        # (MODULE_NAME, f(mod) -> mod version)
        ("addict", lambda mod: mod.__version__),
        ("dask", lambda mod: mod.__version__),
        ("h5netcdf", lambda mod: mod.__version__),
        ("icoscp", lambda mod: mod.__version__),
        ("matplotlib", lambda mod: mod.__version__),
        ("msgpack", lambda mod: mod.__version__),
        ("netcdf4", lambda mod: mod.__version__),
        ("nbformat", lambda mod: mod.__version__),
        ("numexpr", lambda mod: mod.__version__),
        ("numpy", lambda mod: mod.__version__),
        ("nc-time-axis", lambda mod: mod.__version__),
        ("pandas", lambda mod: mod.__version__),
        ("plotly", lambda mod: mod.__version__),
        ("pyvis", lambda mod: mod.__version__),
        ("rapidfuzz", lambda mod: mod.__version__),
        ("requests", lambda mod: mod.__version__),
        ("scipy", lambda mod: mod.__version__),
        ("tinydb", lambda mod: mod.__version__),
        ("toml", lambda mod: mod.__version__),
        ("rich", lambda mod: mod.__version__),
        ("xarray", lambda mod: mod.__version__),
        ("urllib3", lambda mod: mod.__version__),
        # openghg setup/test/doc
        ("setuptools", lambda mod: mod.__version__),
        ("pip", lambda mod: mod.__version__),
        ("conda", lambda mod: mod.__version__),
        ("pytest", lambda mod: mod.__version__),
        ("IPython", lambda mod: mod.__version__),
        ("sphinx", lambda mod: mod.__version__),
    ]

    deps_blob: List[Tuple] = []
    for (modname, ver_f) in deps:
        try:
            if modname in sys.modules:
                mod = sys.modules[modname]
            else:
                mod = importlib.import_module(modname)
        except Exception:
            deps_blob.append((modname, None))
        else:
            try:
                ver = ver_f(mod)
                deps_blob.append((modname, ver))
            except Exception:
                deps_blob.append((modname, "installed"))

    print("\nINSTALLED VERSIONS", file=file)
    print("------------------", file=file)

    for k, stat in sys_info:
        print(f"{k}: {stat}", file=file)

    print("", file=file)
    for k, stat in deps_blob:
        print(f"{k}: {stat}", file=file)
