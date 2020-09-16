.. image:: https://travis-ci.com/rasmunk/jobio.svg?branch=master
    :target: https://travis-ci.com/rasmunk/jobio
.. image:: https://badge.fury.io/py/jobio.svg
    :target: https://badge.fury.io/py/jobio
=====
jobio
=====

jobio is used for executing processes on a Unix-like environment.
The execution runtime at the moment is just a simple shell execution of the job command and the optional arguments.

In addition, the package supports the staging of S3 buckets on the executing node before the process scheduled.
Furthermore, upon a successful execution, the `jobio` package supports the automatic export of persistent results
to the same S3 bucket.

To distinquish between what is the staging inputs in the bucket and the subsequent results,
jobio sets default `bucket_input_prefix='input'` and `bucket_output_prefix='output'` to divide these two sets in the bucket.

Very much in an **Alpha** stage.
Not ready for anything!

------------
Installation
------------

Installation from pypi::

    pip install jobio


Installation from local git repository::

    cd jobio
    pip install .
