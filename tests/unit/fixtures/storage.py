# Copyright 2021 Universität Tübingen, DKFZ and EMBL
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Fixtures for working with the storage dao"""

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple

import requests

from ghga_service_chassis_lib.object_storage_dao import PresignedPostURL
from ghga_service_chassis_lib.s3 import ObjectStorageS3, S3ConfigBase

from . import BASE_DIR
from .utils import calc_md5, generate_random_numeric_string

TEST_CREDENTIALS = {"s3_access_key_id": "test", "s3_secret_access_key": "test"}

TEST_FILE_PATH = BASE_DIR / "test_file.yaml"
TEST_FILE_MD5 = calc_md5(TEST_FILE_PATH)


def get_test_config(s3_endpoint_url: str) -> S3ConfigBase:
    """Get a test config according to the S3ConfigBase model."""
    params = {"s3_endpoint_url": s3_endpoint_url, **TEST_CREDENTIALS}
    return S3ConfigBase(**params)


def create_existing_object_and_bucket(
    storage_client: ObjectStorageS3,
) -> Tuple[str, str]:
    """
    Creates a bucket as well as a contained object and returns the bucket and object id.
    """
    # generate random bucket and object ids
    bucket_id = "myexistingbucket" + generate_random_numeric_string()
    object_id = "myexistingobject" + generate_random_numeric_string()

    # create bucket and upload file:
    storage_client.create_bucket(bucket_id)
    upload_url = storage_client.get_object_upload_url(
        bucket_id=bucket_id, object_id=object_id
    )
    upload_test_file(upload_url)

    return bucket_id, object_id
