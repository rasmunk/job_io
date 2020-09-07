import boto3
import os
import subprocess
import time
from jobio.cli.args import extract_arguments, extract_env_variables
from jobio.defaults import EXECUTE, JOB, S3, BUCKET, STORAGE, JOB_DEFAULT_NAME, JOBIO
from jobio.storage.staging import required_staging_fields, required_staging_values
from jobio.storage.s3 import (
    bucket_exists,
    create_bucket,
    expand_s3_bucket,
    valid_s3_resource_fields,
    required_bucket_fields,
    required_bucket_values,
    required_s3_fields,
    required_s3_values,
    upload_directory_to_s3,
)
from jobio.util import (
    validate_dict_types,
    validate_dict_values,
    remove_dir,
    save_results,
    load_kubernetes_secrets,
)

required_job_fields = {"name": str, "debug": bool, "env_override": bool}

required_job_values = {"name": True, "debug": False, "env_override": False}


required_execute_fields = {
    "command": str,
    "args": list,
    "capture": bool,
    "output_path": str,
}

required_execute_values = {
    "command": True,
    "args": False,
    "capture": False,
    "output_path": False,
}


def process(execute_kwargs=None):
    if not execute_kwargs:
        execute_kwargs = {}

    command = [execute_kwargs["command"]]
    if "args" in execute_kwargs:
        command.extend(execute_kwargs["args"])

    # Subprocess
    result = subprocess.run(command, capture_output=execute_kwargs["capture"])
    output_results = {}
    if hasattr(result, "args"):
        output_results.update({"command": " ".join((getattr(result, "args")))})
    if hasattr(result, "returncode"):
        output_results.update({"returncode": str(getattr(result, "returncode"))})
    if hasattr(result, "stderr"):
        output_results.update({"error": str(getattr(result, "stderr"))})
    if hasattr(result, "stdout"):
        output_results.update({"output": str(getattr(result, "stdout"))})
    return output_results


def submit(args):
    job_dict = vars(extract_arguments(args, [JOB]))
    if "name" not in job_dict or not job_dict["name"]:
        since_epoch = int(time.time())
        job_dict["name"] = "{}-{}".format(JOB_DEFAULT_NAME, since_epoch)

    execute_dict = vars(extract_arguments(args, [EXECUTE]))
    if "env_override" in job_dict and job_dict["env_override"]:
        job_env_dict = vars(
            extract_env_variables(required_job_fields, ["{}_{}".format(JOBIO, JOB)])
        )
        execute_env_dict = vars(
            extract_env_variables(
                required_execute_fields, ["{}_{}".format(JOBIO, EXECUTE)]
            )
        )
        job_dict.update(job_env_dict)
        execute_dict.update(execute_env_dict)

    valid_job_types = validate_dict_types(
        execute_dict, required_fields=required_execute_fields, verbose=job_dict["debug"]
    )
    valid_job_values = validate_dict_values(
        execute_dict, required_values=required_execute_values, verbose=job_dict["debug"]
    )

    if not valid_job_types:
        raise TypeError(
            "Incorrect required executable arguments "
            "were provided: {}, required: {}".format(
                execute_dict, required_execute_fields.keys()
            )
        )
    if not valid_job_values:
        raise ValueError(
            "Missing required executable values: {}, required: {}".format(
                execute_dict, required_execute_values.keys()
            )
        )

    staging_storage_dict = vars(extract_arguments(args, [STORAGE]))
    bucket_dict = vars(extract_arguments(args, [BUCKET]))
    s3_dict = vars(extract_arguments(args, [S3]))

    if "env_override" in job_dict and job_dict["env_override"]:
        staging_storage_env_dict = vars(
            extract_env_variables(
                required_staging_fields, ["{}_{}".format(JOBIO, STORAGE)]
            )
        )

        bucket_env_dict = vars(
            extract_env_variables(
                required_bucket_fields, ["{}_{}".format(JOBIO, BUCKET)]
            )
        )

        s3_env_dict = vars(
            extract_env_variables(required_s3_fields, ["{}_{}".format(JOBIO, S3)])
        )

        staging_storage_dict.update(staging_storage_env_dict)
        bucket_dict.update(bucket_env_dict)
        s3_dict.update(s3_env_dict)

    # Dynamically get secret credentials
    if staging_storage_dict["enable"]:
        validate_dict_types(
            staging_storage_dict,
            required_fields=required_staging_fields,
            verbose=job_dict["debug"],
            throw=True,
        )
        validate_dict_values(
            staging_storage_dict,
            required_values=required_staging_values,
            verbose=job_dict["debug"],
            throw=True,
        )

        # Validate bucket arguments
        validate_dict_types(
            bucket_dict,
            required_fields=required_bucket_fields,
            verbose=job_dict["debug"],
            throw=True,
        )

        validate_dict_values(
            bucket_dict,
            required_values=required_bucket_values,
            verbose=job_dict["debug"],
            throw=True,
        )

        # Validate staging arguments
        load_secrets = ["aws_access_key_id", "aws_secret_access_key"]
        loaded_secrests = load_kubernetes_secrets(
            staging_storage_dict["secrets_dir"], load_secrets
        )
        for k, v in loaded_secrests.items():
            s3_dict.update({k: v})

        validate_dict_types(
            s3_dict,
            required_fields=required_s3_fields,
            verbose=job_dict["debug"],
            throw=True,
        )
        validate_dict_values(
            s3_dict,
            required_values=required_s3_values,
            verbose=job_dict["debug"],
            throw=True,
        )

        # Only used valid_s3_fields
        resources_fields = {
            k: v
            for k, v in s3_dict.items()
            if k in valid_s3_resource_fields or k in load_secrets
        }

        # Add the s3 endpoint_url to the resource config
        if resources_fields:
            resources_fields.update(dict(endpoint_url=staging_storage_dict["endpoint"]))

        if job_dict["debug"]:
            print("Storage enable debug, S3 Options: {}".format(s3_dict))
            print("Storage enable debug, Resource Options: {}".format(resources_fields))
            print(
                "Storage enable debug, Staging Options: {}".format(staging_storage_dict)
            )
            print("Storage enable debug, Bucket Options: {}".format(bucket_dict))

        s3_resource = boto3.resource("s3", **resources_fields)
        # Load aws credentials
        expanded = expand_s3_bucket(
            s3_resource,
            bucket_dict["name"],
            target_dir=staging_storage_dict["input_path"],
            s3_prefix=bucket_dict["input_prefix"],
        )
        if not expanded:
            raise RuntimeError(
                "Failed to expand the target bucket: {}".format(bucket_dict["name"])
            )

    result = process(execute_kwargs=execute_dict)
    saved = False
    final_result_path = ""

    # Put results into the put path
    if "output_path" in execute_dict:
        root, ext = os.path.splitext(execute_dict["output_path"])
        if ext:
            final_result_path = execute_dict["output_path"]
        else:
            # A directory path
            # Generate a filename to put inside the directory
            result_output_file = "{}.txt".format(job_dict["name"])
            final_result_path = os.path.join(
                execute_dict["output_path"], result_output_file
            )
        if final_result_path:
            saved = save_results(final_result_path, result)
            if not saved:
                raise RuntimeError(
                    "Failed to save the results of job: {}".format(job_dict["name"])
                )

    if staging_storage_dict["enable"]:
        if not bucket_exists(s3_resource.meta.client, bucket_dict["name"]):
            # TODO, load region from AWS config
            created = create_bucket(
                s3_resource.meta.client,
                bucket_dict["name"],
                CreateBucketConfiguration={"LocationConstraint": s3_dict["region"]},
            )
            if not created:
                raise RuntimeError(
                    "Failed to create results bucket: {}".format(bucket_dict["name"])
                )

        uploaded = upload_directory_to_s3(
            s3_resource.meta.client,
            staging_storage_dict["output_path"],
            bucket_dict["name"],
            s3_prefix=bucket_dict["output_prefix"],
        )
        if not uploaded:
            print("Failed to upload results")

        # Cleanout local results
        if os.path.exists(os.path.dirname(final_result_path)):
            if not remove_dir(os.path.dirname(final_result_path)):
                print("Failed to remove results after upload")
        # TODO, cleanout inputs
        if os.path.exists(staging_storage_dict["input_path"]):
            if not remove_dir(staging_storage_dict["input_path"]):
                print("Failed to remove input after upload")

    print("{}".format(result))
    # TODO, print output to stdout
