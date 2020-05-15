import os
import base64
import binascii
import boto3
import botocore
import json
import shutil
import subprocess
from args import get_arguments, JOB, S3


def create_dir(path):
    try:
        os.makedirs(path)
        return True
    except IOError as err:
        print("Failed to create: {}, err: {}".format(path, err))
    return False


def remove_dir(path):
    try:
        shutil.rmtree(path)
        return True
    except OSError as err:
        print("Failed to remove: {}, err: {}".format(path, err))
    return False


# Accept parameters to
def expand_s3_bucket(s3_resource, bucket_name, target_dir=None, prefix="input"):
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        path = os.path.dirname(obj.key)
        if target_dir:
            path = os.path.join(target_dir, obj.key)

        dir_path = os.path.dirname(path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        bucket.download_file(obj.key, path)
    return True


def upload_to_s3(s3_client, local_path, s3_path, bucket_name):
    if not os.path.exists(local_path):
        return False
    s3_client.upload_file(local_path, bucket_name, s3_path)
    return True


def upload_directory(s3_client, path, bucket_name, s3_prefix="output"):
    if not os.path.exists(path):
        return False
    for root, dirs, files in os.walk(path):
        for filename in files:
            local_path = os.path.join(root, filename)
            # generate s3 dir path
            # Skip the first /
            # HACK
            s3_path = local_path.split(path)[1][1:]
            if s3_prefix:
                s3_path = os.path.join(s3_prefix, s3_path)
            # Upload
            uploaded = upload_to_s3(s3_client, local_path, s3_path, bucket_name)
            if not uploaded:
                return False
    return True


def load_s3_config(path, s3_config):
    loaded_settings = {}
    # Find keys
    for k, v in s3_config.items():
        setting_path = os.path.join(path, k)
        if os.path.exists(setting_path):
            with open(setting_path, "r") as _file:
                content = _file.read()
                # If base64 string
                try:
                    content = base64.decodestring(content)
                except binascii.Error:
                    pass
                loaded_settings[k] = content
    return loaded_settings


def save_results(path, results):
    save_dir = os.path.dirname(path)
    if not os.path.exists(save_dir):
        if not create_dir(save_dir):
            return False
    try:
        with open(path, "w") as fh:
            try:
                json.dump(results, fh)
            except TypeError as j_err:
                print("Failed to serialize to json: {}".format(j_err))
                return False
        return True
    except IOError as err:
        print("Failed to save results: {}".format(err))
    return False


def process(job_kwargs=None):
    if not job_kwargs:
        job_kwargs = {}

    command = [job_kwargs.command]
    if job_kwargs.args:
        command.extend(job_kwargs.args)

    # Subprocess
    result = subprocess.run(command, capture_output=job_kwargs.verbose_output)

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


def bucket_exists(s3_client, bucket_name):
    try:
        bucket = s3_client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError:
        pass
    return False


def create_bucket(s3_client, bucket_name, **kwargs):
    bucket = s3_client.create_bucket(Bucket=bucket_name, **kwargs)
    if not bucket:
        return False
    return bucket


# Set parameters via yaml or environment
if __name__ == "__main__":
    job_args = get_arguments([JOB], strip_group_prefix=True)
    s3_args = get_arguments([S3], strip_group_prefix=True)
    s3_config = {}
    if s3_args.endpoint_url:
        s3_config.update({"endpoint_url": s3_args.endpoint_url})

    # Load aws credentials
    s3_resource = boto3.resource("s3", **s3_config)
    expanded = expand_s3_bucket(
        s3_resource, s3_args.bucket_name, target_dir=s3_args.input_path
    )
    if not expanded:
        exit(1)

    result = process(job_kwargs=job_args)

    saved = False
    # Put results into the put path
    result_output_file = "{}.txt".format(job_args.name)
    relative_path = os.path.join("output", result_output_file)
    if s3_args.output_path:
        full_result_path = os.path.join(s3_args.output_path, relative_path)
    else:
        full_result_path = os.path.abspath(relative_path)
    saved = save_results(full_result_path, result)

    if saved:
        if not bucket_exists(s3_resource.meta.client, s3_args.bucket_name):
            # TODO, load region from AWS config
            created = create_bucket(
                s3_resource.meta.client,
                s3_args.bucket_name,
                CreateBucketConfiguration={"LocationConstraint": "eu-amsterdam-1"},
            )
            if not created:
                print("Failed to create results bucket: {}".format(s3_args.bucket_name))
                exit(1)

        uploaded = upload_directory(
            s3_resource.meta.client, s3_args.output_path, s3_args.bucket_name
        )
        if not uploaded:
            print("Failed to upload results")
            exit(1)

        # Cleanout local results
        if os.path.exists(os.path.dirname(full_result_path)):
            if not remove_dir(os.path.dirname(full_result_path)):
                print("Failed to remove results after upload")
                exit(1)
