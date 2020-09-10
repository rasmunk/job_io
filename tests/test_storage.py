# import unittest
# import os
# from jobio.util import validate_dict_types, validate_dict_values, create_dir
# from jobio.storage.staging import required_staging_fields, required_staging_values

# from jobio.storage.s3 import (
#     required_s3_fields,
#     required_s3_values,
#     required_bucket_fields,
#     required_bucket_values,
#     upload_directory_to_s3,
#     expand_s3_bucket,
#     stage_s3_resource,
#     create_bucket,
#     delete_bucket,
#     delete_objects,
# )


# here = os.path.abspath(__file__)
# current_dir = os.path.dirname(here)


# class TestStorage(unittest.TestCase):
#     def setUp(self):
#         self.staging_options = dict(
#             enable=True,
#             secrets_dir="",
#             input_path="",
#             output_path="results",
#             endpoint="https://ku.compat.objectstorage.eu-frankfurt-1"
#             ".oraclecloud.com",
#         )

#         self.s3_options = dict(region_name="eu-frankfurt-1")

#         self.bucket_options = dict(
#             name="bucket_test_name",
#             input_prefix="input",
#             output_prefix="output",
#         )

#     def tearDown(self):
#         self.staging_options = None
#         self.s3_options = None

#     def test_staging_options(self):
#         self.assertTrue(
#             validate_dict_types(
#                 self.staging_options,
#                 required_fields=required_staging_fields,
#             )
#         )

#         self.assertTrue(
#             validate_dict_values(
#                 self.staging_options, required_values=required_staging_values
#             )
#         )

#     def test_s3_options(self):
#         self.assertTrue(
#             validate_dict_types(self.s3_options, required_fields=required_s3_fields)
#         )

#         self.assertTrue(
#             validate_dict_values(self.s3_options, required_values=required_s3_values)
#         )

#     def test_bucket_options(self):
#         self.assertTrue(
#             validate_dict_types(
#                 self.bucket_options, required_fields=required_bucket_fields
#             )
#         )

#         self.assertTrue(
#             validate_dict_values(
#                 self.bucket_options, required_values=required_bucket_values
#             )
#         )

#     def test_s3_resource(self):
#         s3_resource = stage_s3_resource(
#             endpoint_url=self.staging_options["endpoint"], **self.s3_options
#         )
#         self.assertIsNotNone(s3_resource)

#     def test_create_delete_bucket(self):
#         s3_resource = stage_s3_resource(
#             endpoint_url=self.staging_options["endpoint"], **self.s3_options
#         )
#         self.assertIsNotNone(s3_resource)

#         bucket = create_bucket(s3_resource.meta.client, self.bucket_options["name"])

#         self.assertIsInstance(bucket, dict)
#         deleted = delete_bucket(s3_resource.meta.client, self.bucket_options["name"])

#         self.assertIsInstance(deleted, dict)
#         self.assertEqual(204, deleted["ResponseMetadata"]["HTTPStatusCode"])

#     def test_upload_expand_bucket(self):
#         # Expand test directory
#         s3_resource = stage_s3_resource(
#             endpoint_url=self.staging_options["endpoint"], **self.s3_options
#         )

#         bucket_options = dict(
#             name="jobio",
#             input_prefix="input",
#             output_prefix="output",
#         )
#         local_directory = os.path.join(current_dir, "res", "job_input")

#         bucket = create_bucket(s3_resource.meta.client, bucket_options["name"])
#         self.assertIsInstance(bucket, dict)

#         uploaded = upload_directory_to_s3(
#             s3_resource.meta.client, local_directory, bucket_options["name"]
#         )
#         self.assertTrue(uploaded)

#         expand_dir = os.path.join(current_dir, "res", "expand_dir")
#         if not os.path.exists(expand_dir):
#             self.assertTrue(create_dir(expand_dir))

#         expaned = expand_s3_bucket(s3_resource, bucket_options["name"], expand_dir)
#         self.assertTrue(expaned)

#         deleted = delete_objects(s3_resource, bucket_options["name"])

#         # Delete all content
#         deleted = delete_bucket(s3_resource.meta.client, bucket_options["name"])
#         self.assertIsInstance(deleted, dict)
#         self.assertEqual(204, deleted["ResponseMetadata"]["HTTPStatusCode"])
