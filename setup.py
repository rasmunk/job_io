#!/usr/bin/python
# coding: utf-8

import os
from setuptools import setup, find_packages

cur_dir = os.path.abspath(os.path.dirname(__file__))

version_ns = {}
with open(os.path.join(cur_dir, "version.py")) as f:
    exec(f.read(), {}, version_ns)

long_description = open("README.rst").read()
setup(
    name="job_io",
    version=version_ns["__version__"],
    description="A bootstrap package to handle input and output data for batch processes",
    long_description=long_description,
    author="Rasmus Munk",
    author_email="munk1@live.dk",
    packages=find_packages(),
    url="https://github.com/rasmunk/job_io",
    license="MIT",
    keywords=["Job", "IO", "S3", "Batch"],
    install_requires=["boto3"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
