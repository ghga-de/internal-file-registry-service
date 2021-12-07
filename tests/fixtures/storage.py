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

"""Storage fixtures"""

from ghga_service_chassis_lib.object_storage_dao_testing import ObjectFixture
from ghga_service_chassis_lib.utils import TEST_FILE_PATHS

from .config import DEFAULT_CONFIG

EXAMPLE_STUDY_ID = "mystudy-123456789"

EXISTING_BUCKETS = [DEFAULT_CONFIG.s3_out_stage_bucket_id, EXAMPLE_STUDY_ID]

EXISTING_OBJECTS = [
    # File located only in the permanent storage:
    ObjectFixture(
        file_path=TEST_FILE_PATHS[0],
        bucket_id=EXAMPLE_STUDY_ID,
        object_id="myfile-11111111",
    ),
    # File located in both the permanent storage and the stage
    ObjectFixture(
        file_path=TEST_FILE_PATHS[1],
        bucket_id=EXAMPLE_STUDY_ID,
        object_id="myfile-22222222",
    ),
    ObjectFixture(
        file_path=TEST_FILE_PATHS[1],
        bucket_id=DEFAULT_CONFIG.s3_out_stage_bucket_id,
        object_id="myfile-22222222",
    ),
]
