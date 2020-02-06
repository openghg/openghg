import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hugs",
    version="0.0.3",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="A HUb for greenhouse Gas Data Science",
    data_files=[{"Data", ["HUGS/Data/acrg_with_locations.json"]}]
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chryswoods/hugs",
    # packages=setuptools.find_packages(),
    packages=['HUGS'],
    package_dir={'HUGS': 'HUGS'},
    package_data={'HUGS': ['HUGS/Data/*.json']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: TBA",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
