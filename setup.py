import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hugs",
    version="0.0.2",
    author="Gareth Jones",
    author_email="g.m.jones@bristol.ac.uk",
    description="A HUb for greenhouse Gas Data Science",
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
        "tables",
    ]
    python_requires=">=3.6",
)
