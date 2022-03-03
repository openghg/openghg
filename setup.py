import setuptools
import sys

sys.path.insert(0, ".")  # noqa
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

files = ["openghg/*"]

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setuptools.setup(
    version="1.0.0",
    install_requires=requirements,
    name="openghg",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="OpenGHG - a cloud platform for greenhouse gas data analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/openghg/openghg",
    packages=setuptools.find_packages(include=["openghg", "openghg.*"]),
    package_data={"": ["data/*"]},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS",
    ],
    python_requires=">=3.7",
)
