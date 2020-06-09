import argparse
from argparse import Namespace

from jobio.defaults import EXECUTE, JOB, S3, STORAGE, BUCKET


def strip_argument_prefix(arguments, prefix=""):
    return {k.replace(prefix, ""): v for k, v in arguments.items()}


def _get_arguments(arguments, startswith=""):
    return {k: v for k, v in arguments.items() if k.startswith(startswith)}


def add_job_meta_group(parser):
    meta_group = parser.add_argument_group(title="Job metadata")
    meta_group.add_argument("--job-name", default=False)
    meta_group.add_argument("--job-debug", action="store_true", default=False)


def add_execute_group(parser):
    execute_group = parser.add_argument_group(title="Execute arguments")
    execute_group.add_argument("execute_command", default="")
    execute_group.add_argument("--execute-args", nargs="*", default=[])
    execute_group.add_argument("--execute-capture", action="store_true", default=True)
    execute_group.add_argument("--execute-output-path", default="")


def add_bucket_group(parser):
    bucket_group = parser.add_argument_group(title="Bucket arguments")
    bucket_group.add_argument("--bucket-name", default="")
    bucket_group.add_argument("--bucket-input-prefix", default="input")
    bucket_group.add_argument("--bucket-output-prefix", default="output")


def add_s3_group(parser):
    s3_group = parser.add_argument_group(title="S3 arguments")
    s3_group.add_argument("--s3-profile-name", default="default")
    s3_group.add_argument("--s3-region-name", default="eu-frankfurt-1")


def add_storage_group(parser):
    storage_group = parser.add_argument_group(title="Staging Storage arguments")
    storage_providers = storage_group.add_mutually_exclusive_group()
    storage_providers.add_argument("--storage-s3", action="store_true")
    storage_group.add_argument("--storage-enable", action="store_true", default=False)
    storage_group.add_argument("--storage-endpoint", default="")
    storage_group.add_argument("--storage-secrets-dir", default="")
    storage_group.add_argument("--storage-input-path", default="/tmp/input")
    storage_group.add_argument("--storage-output-path", default="/tmp/output")


argument_groups = {
    EXECUTE: add_execute_group,
    JOB: add_job_meta_group,
    STORAGE: add_storage_group,
    S3: add_s3_group,
    BUCKET: add_bucket_group,
}


def get_arguments(argument_types, strip_group_prefix=True):
    parser = argparse.ArgumentParser()

    for argument_group in argument_types:
        if argument_group in argument_groups:
            argument_groups[argument_group](parser)

    args, unknown = parser.parse_known_args()
    if strip_group_prefix:
        stripped_args = {}
        for argument_group in argument_types:
            group_args = _get_arguments(vars(args), argument_group.lower())
            group_args = strip_argument_prefix(group_args, argument_group.lower() + "_")
            stripped_args.update(group_args)
        return Namespace(**stripped_args)
    return args


def extract_arguments(arguments, argument_types, strip_group_prefix=True):
    if strip_group_prefix:
        stripped_args = {}
        for argument_group in argument_types:
            group_args = _get_arguments(vars(arguments), argument_group.lower())
            group_args = strip_argument_prefix(group_args, argument_group.lower() + "_")
            stripped_args.update(group_args)
        return Namespace(**stripped_args)
    return {}
