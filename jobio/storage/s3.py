import botocore
import copy
import os


required_s3_fields = {
    "bucket_name": str,
    "bucket_input_prefix": str,
    "bucket_output_prefix": str,
    "profile_name": str,
    "region_name": str,
}

required_s3_values = {
    "bucket_name": False,
    "bucket_input_prefix": False,
    "bucket_output_prefix": False,
    "profile_name": True,
    "region_name": True,
}


def load_s3_session_vars(directory, session_vars, strip_newline=True):
    loaded_settings = {}
    for k, v in session_vars.items():
        value_path = os.path.join(directory, k)
        if os.path.exists(value_path):
            if os.path.islink(value_path):
                value_path = os.path.realpath(value_path)
            if os.path.isfile(value_path) and not os.path.islink(value_path):
                content = None
                try:
                    with open(value_path, "rb") as fh:
                        content = fh.read()
                except IOError as err:
                    print("Failed to read file: {}".format(err))
                decoded = content.decode("utf-8")
                if strip_newline:
                    decoded = decoded.replace("\n", "")
                loaded_settings[k] = decoded
    return loaded_settings


def upload_to_s3(s3_client, local_path, s3_path, bucket_name):
    if not os.path.exists(local_path):
        return False
    s3_client.upload_file(local_path, bucket_name, s3_path)
    return True


def upload_directory_to_s3(client, path, bucket_name, s3_prefix="input"):
    if not os.path.exists(path):
        return False
    for root, dirs, files in os.walk(path):
        for filename in files:
            local_path = os.path.join(root, filename)
            # generate s3 dir path
            # Skip the first /
            # HACK
            s3_path = local_path.split(path)[1][1:]
            # Upload
            if s3_prefix:
                s3_path = os.path.join(s3_prefix, s3_path)
            if not client.upload_file(local_path, bucket_name, s3_path):
                return False
    return True


def bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except botocore.exceptions.ClientError:
        pass
    return False


def create_bucket(s3_client, bucket_name, **kwargs):
    bucket = s3_client.create_bucket(Bucket=bucket_name, **kwargs)
    if not bucket:
        return False
    return bucket


# Accept parameters to
def expand_s3_bucket(s3_resource, bucket_name, target_dir=None, prefix="input"):
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        obj_path = copy.deepcopy(obj.key)
        if prefix:
            obj_path = os.path.relpath(obj_path, prefix)

        if target_dir:
            full_path = os.path.join(target_dir, obj_path)
        else:
            full_path = obj_path
        dir_path = os.path.dirname(full_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        bucket.download_file(obj.key, full_path)
    return True
