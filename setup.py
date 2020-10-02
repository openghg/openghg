import setuptools
import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

files = ["openghg/*"]

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open('requirements-dev.txt') as f:
    dev_requirements = f.read().splitlines()

setuptools.setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=requirements,
    extras_require={
        "dev": dev_requirements,
        "docs": ["sphinx", "sphinx-rtd-theme"]
    },
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
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: TBA",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
