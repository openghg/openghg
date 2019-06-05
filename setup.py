import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hugs",
    version="0.0.1",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="A Hub for UK Greenhouse Gas Data Science",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chryswoods/hugs",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: TBA",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "acquire",
        "tables"
    ]
)
