# type: ignore
import sys

import setuptools

sys.path.insert(0, ".")  # noqa
import versioneer  # noqa

with open("README.md", "r") as fh:
    long_description = fh.read()

files = ["openghg/*"]

with open("requirements.txt") as f:
    requirements = f.read().splitlines()


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
    package_data={"": ["data/*", "data/config/objectstore/*", "py.typed"]},
    classifiers=[
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
    python_requires=">=3.10",
)
