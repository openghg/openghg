import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

files = ["HUGS/*"]

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name="hugs",
    version="0.0.3",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="A HUb for greenhouse Gas data Science",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chryswoods/hugs",
    # packages=setuptools.find_packages(),
    packages=setuptools.find_packages(include=["HUGS", "HUGS.*"]),
    package_data={"": ["Data/*"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: TBA",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=requirements
)
