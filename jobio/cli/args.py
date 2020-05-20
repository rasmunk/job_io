import argparse
from argparse import Namespace

from jobio.defaults import RUN, S3, STAGING_STORAGE


def strip_argument_prefix(arguments, prefix=""):
    return {k.replace(prefix, ""): v for k, v in arguments.items()}


def _get_arguments(arguments, startswith=""):
    return {k: v for k, v in arguments.items() if k.startswith(startswith)}


def add_execute_group(parser):
    execute_group = parser.add_argument_group(title="Execute arguments")
    execute_group.add_argument("execute_command", default="")
    execute_group.add_argument("--execute-args", nargs="*", default="")
    execute_group.add_argument("--execute-verbose", default=False)
    execute_group.add_argument("--execute-output-path", default="/tmp/output")


def add_s3_group(parser):
    s3_group = parser.add_argument_group(title="S3 arguments")
    s3_group.add_argument("--s3-bucket-name", default="")
    s3_group.add_argument("--s3-bucket-input-prefix", default="input")
    s3_group.add_argument("--s3-bucket-output-prefix", default="output")
    s3_group.add_argument("--s3-profile-name", default="default")


def add_storage_group(parser):
    storage_group = parser.add_argument_group(title="Storage arguments")
    storage_providers = storage_group.add_mutually_exclusive_group()
    storage_providers.add_argument("--storage-s3", default=False, action="store_true")
    storage_group.add_argument("--storage-endpoint", default="")
    storage_group.add_argument("--storage-session-vars", default="")
    storage_group.add_argument("--storage-input-path", default="/tmp/input")
    storage_group.add_argument("--storage-output-path", default="/tmp/output")


argument_groups = {
    RUN: add_execute_group,
    STAGING_STORAGE: add_storage_group,
    S3: add_s3_group,
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
