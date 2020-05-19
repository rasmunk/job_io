import botocore
import os


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
