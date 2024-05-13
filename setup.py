# type: ignore
import pathlib
import setuptools
import sys

sys.path.insert(0, ".")  # noqa
import versioneer  # noqa

with open("README.md", "r") as fh:
    long_description = fh.read()

files = ["openghg/*"]

with open("requirements.txt") as f:
    requirements = f.read().splitlines()


def create_package_data():
    """Allows us to add all of the contents of the data folder
    as setuptools doesn't support recursive globbing. If we move to
    pyproject.toml properly we may be able to remove this.
    """
    data_files = [p for p in pathlib.Path("data").rglob("*")]
    data_files.append("py.typed")
    return data_files


setuptools.setup(
    version=versioneer.get_version(),
    install_requires=requirements,
    name="openghg",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="OpenGHG - a cloud platform for greenhouse gas data analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/openghg/openghg",
    packages=setuptools.find_packages(include=["openghg", "openghg.*"]),
    package_data={"": create_package_data()},
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
    ],
    entry_points={
        "console_scripts": [
            "openghg = openghg.util:cli",
        ]
    },
    python_requires=">=3.9",
)
