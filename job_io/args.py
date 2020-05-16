import argparse
from argparse import Namespace

JOB = "JOB"
S3 = "S3"


def strip_argument_prefix(arguments, prefix=""):
    return {k.replace(prefix, ""): v for k, v in arguments.items()}


def _get_arguments(arguments, startswith=""):
    return {k: v for k, v in arguments.items() if k.startswith(startswith)}


def add_job_group(parser):
    job_group = parser.add_argument_group(title="JOB arguments")
    job_group.add_argument("--job-name", default="DEFAULT")
    job_group.add_argument("--job-command", default="")
    job_group.add_argument("--job-args", nargs="*", default="")
    job_group.add_argument("--job-verbose-output", default=True)


def add_s3_group(parser):
    s3_group = parser.add_argument_group(title="S3 arguments")
    s3_group.add_argument("--s3-session-vars", default=False)
    s3_group.add_argument("--s3-endpoint-url", default=False)
    s3_group.add_argument("--s3-region-name", default=False)
    s3_group.add_argument("--s3-bucket-name", default=False)
    s3_group.add_argument("--s3-input-path", default="/tmp/input")
    s3_group.add_argument("--s3-output-path", default="/tmp/output")


argument_groups = {JOB: add_job_group, S3: add_s3_group}


def get_arguments(argument_types, strip_group_prefix=False):
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
