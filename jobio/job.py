import boto3
import os
import subprocess
from jobio.cli.args import extract_arguments
from jobio.defaults import JOB, S3, STAGING_STORAGE
from jobio.storage.staging import required_staging_fields, required_staging_values
from jobio.storage.s3 import (
    bucket_exists,
    create_bucket,
    expand_s3_bucket,
    required_s3_fields,
    required_s3_values,
    upload_directory_to_s3,
    load_s3_session_vars,
)
from jobio.util import (
    validate_dict_types,
    validate_dict_values,
    remove_dir,
    save_results,
)

required_job_fields = {
    "command": str,
    "args": list,
    "verbose": bool,
    "output_path": str,
}

required_job_values = {
    "command": True,
    "args": False,
    "verbose": False,
    "output_path": False,
}


def process(job_kwargs=None):
    if not job_kwargs:
        job_kwargs = {}

    command = [job_kwargs["command"]]
    if "args" in job_kwargs:
        command.extend(job_kwargs["args"])

    # Subprocess
    result = subprocess.run(command, capture_output=job_kwargs["verbose_output"])
    output_results = {}
    if hasattr(result, "args"):
        output_results.update({"command": str(getattr(result, "args"))})
    if hasattr(result, "returncode"):
        output_results.update({"returncode": str(getattr(result, "returncode"))})
    if hasattr(result, "stderr"):
        output_results.update({"error": str(getattr(result, "stderr"))})
    if hasattr(result, "stdout"):
        output_results.update({"output": str(getattr(result, "stdout"))})
    return output_results


def submit(args):
    job_dict = vars(extract_arguments(args, [JOB]))
    valid_job_types = validate_dict_types(job_dict, required_job_fields)
    valid_job_values = validate_dict_values(job_dict, required_job_values)

    if not valid_job_types:
        raise TypeError(
            "Incorrect required job arguments were provided: {}".format(valid_job_types)
        )

    if not valid_job_values:
        raise ValueError("Missing required job arguments: {}".format(valid_job_values))

    staging_storage_dict = vars(extract_arguments(args, [STAGING_STORAGE]))
    s3_dict = vars(extract_arguments(args, [S3]))

    valid_staging_types = validate_dict_types(
        staging_storage_dict, required_staging_fields
    )
    valid_staging_values = validate_dict_values(
        staging_storage_dict, required_staging_values
    )

    # Dynamically get secret credentials
    if valid_staging_types and valid_staging_values:
        load_session_vars = dict(aws_access_key_id=None, aws_secret_access_key=None)
        loaded_session_vars = load_s3_session_vars(
            staging_storage_dict["session_vars"], load_session_vars
        )
        for k, v in loaded_session_vars.items():
            s3_dict.update({k: v})

    valid_s3_types = validate_dict_types(s3_dict, required_s3_fields)
    valid_s3_values = validate_dict_values(s3_dict, required_s3_values)

    if valid_s3_types and valid_s3_values:
        s3_resource = boto3.resource("s3", **s3_dict)
        # Load aws credentials
        expanded = expand_s3_bucket(
            s3_resource,
            s3_dict["bucket_name"],
            target_dir=staging_storage_dict["input_path"],
        )
        if not expanded:
            raise RuntimeError(
                "Failed to expand the target bucket: {}".format(s3_dict["bucket_name"])
            )

    result = process(job_kwargs=job_dict)
    saved = False
    # Put results into the put path
    result_output_file = "{}.txt".format(job_dict["name"])

    if valid_staging_types and valid_staging_values:
        full_result_path = os.path.join(
            staging_storage_dict["output_path"], result_output_file
        )
    else:
        full_result_path = os.path.abspath(result_output_file)
    saved = save_results(full_result_path, result)

    if not saved:
        raise RuntimeError(
            "Failed to save the results of job: {}".format(job_dict["name"])
        )

    print("Saved results for: {}".format(job_dict["name"]))

    if (
        valid_s3_types
        and valid_s3_values
        and valid_staging_types
        and valid_staging_values
    ):
        if not bucket_exists(s3_resource.meta.client, s3_dict["bucket_name"]):
            # TODO, load region from AWS config
            created = create_bucket(
                s3_resource.meta.client,
                s3_dict["bucket_name"],
                CreateBucketConfiguration={"LocationConstraint": s3_dict["region"]},
            )
            if not created:
                raise RuntimeError(
                    "Failed to create results bucket: {}".format(s3_dict["bucket_name"])
                )

        uploaded = upload_directory_to_s3(
            s3_resource.meta.client,
            staging_storage_dict["output_path"],
            s3_dict["bucket_name"],
        )
        if not uploaded:
            print("Failed to upload results")

        # Cleanout local results
        if os.path.exists(os.path.dirname(full_result_path)):
            if not remove_dir(os.path.dirname(full_result_path)):
                print("Failed to remove results after upload")
        # TODO, cleanout inputs
        if os.path.exists(staging_storage_dict["input_path"]):
            if not remove_dir(staging_storage_dict["input_path"]):
                print("Failed to remove input after upload")

    # TODO, print output to stdout
    print("Finished")
