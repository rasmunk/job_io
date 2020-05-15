import os
import base64
import binascii
import boto3
import subprocess
from job_io.args import get_arguments, JOB, S3


# Accept parameters to
def expand_s3_bucket(s3_client, bucket_name, expand_path=None):
    bucket_object = s3_client.get_object(bucket)
    # TODO, extract all content into `expand_path`

def upload_s3(s3_client, data, s3_kwargs=None):
    if not data:
        return False

    if not s3_kwargs:
        return False


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


def save_s3_config(s3_config):
    pass


def process(job_kwargs=None, s3_kwargs=None):
    if not job_kwargs:
        job_kwargs = {}

    if not s3_kwargs:
        s3_kwargs = {}

    # Subprocess
    result = subprocess.run(
        job_kwargs["command"], capture_output=job_kwargs["job_verbose_output"]
    )
    if result.returncode != 0:
        return False, (result.returncode, result.stdout, result.stderr)

    return True, result.stdout


# Set parameters via yaml or environment
if __name__ == "__main__":
    s3_settings = dict(
        aws_access_key=None, aws_secret_access_key=None, region=None, endpoint_url=None
    )
    s3_config_path = os.path.join(os.sep, "mnt", "s3_config")
    s3_config = load_s3_config(s3_config_path, s3_settings)

    job_args = get_arguments([JOB], strip_group_prefix=True)
    s3_args = get_arguments([S3], strip_group_prefix=True)

    s3_client = None
    # save_s3_config(s3_config)
    expanded = expand_s3_bucket(
        s3_client, s3_args.bucket_name, expand_path=s3_args.input_path
    )
    if not expanded:
        exit(1)

    success, result = process(job_kwargs=job_args, s3_kwargs=s3_args)

    uploaded = upload_s3(result, s3_kwargs=s3_args)
    if not uploaded:
        print("Failed to upload results")
        exit(1)
