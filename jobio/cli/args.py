import argparse
import os
import textwrap
from argparse import Namespace, Action
from jobio.defaults import JOB, JOB_META, S3, STORAGE, BUCKET


class CommandAction(Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if hasattr(namespace, "job_commands"):
            existing_values = getattr(namespace, "job_commands")
            try:
                existing_values.extend(values)
            except AttributeError:
                existing_values = values
            finally:
                setattr(namespace, "job_commands", existing_values)
        else:
            setattr(namespace, "job_commands", values)


def strip_argument_prefix(arguments, prefix=""):
    return {k.replace(prefix, ""): v for k, v in arguments.items()}


def _get_arguments(arguments, startswith=""):
    return {k: v for k, v in arguments.items() if k.startswith(startswith)}


def _get_env_variables(variables, startswith=""):
    env_vars = {}
    for var_key, var_type in variables.items():
        env_argument = "{}_{}".format(startswith, var_key.upper())
        if env_argument in os.environ:
            # Ensure the environment value is cast to the correct type
            value = os.environ[env_argument]
            if isinstance(var_type(), list):
                env_vars[var_key] = textwrap.wrap(value)
            elif isinstance(var_type(), bool):
                if value == "True" or value == "1":
                    env_vars[var_key] = True
                else:
                    env_vars[var_key] = False
            else:
                env_vars[var_key] = value
    return env_vars


def add_job_meta_group(parser):
    meta_group = parser.add_argument_group(title="Job metadata")
    meta_group.add_argument("--job-meta-name", default=False)
    meta_group.add_argument("--job-meta-debug", action="store_true", default=False)
    meta_group.add_argument(
        "--job-meta-env-override", action="store_true", default=False
    )


def add_job_group(parser):
    job_group = parser.add_argument_group(title="Job arguments")
    job_group.add_argument("job_commands", nargs="+", action=CommandAction)
    job_group.add_argument("--job-capture", action="store_true", default=True)
    job_group.add_argument("--job-output-path", default="")


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
    JOB: add_job_group,
    JOB_META: add_job_meta_group,
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


def extract_env_variables(variables, variable_types, strip_group_prefix=True):
    if strip_group_prefix:
        stripped_env_vars = {}
        for variable_group in variable_types:
            group_args = _get_env_variables(variables, variable_group.upper())
            group_args = strip_argument_prefix(group_args, variable_group.lower() + "_")
            stripped_env_vars.update(group_args)
        return Namespace(**stripped_env_vars)
    return {}
