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

"""Fixtures for testing the storage DAO"""

from ghga_service_chassis_lib.object_storage_dao_testing import (
    DEFAULT_EXISTING_BUCKETS,
    DEFAULT_EXISTING_OBJECTS,
    DEFAULT_NON_EXISTING_BUCKETS,
    DEFAULT_NON_EXISTING_OBJECTS,
)
from ghga_service_chassis_lib.s3_testing import s3_fixture_factory

from .config import DEFAULT_CONFIG

EXISTING_BUCKETS = DEFAULT_EXISTING_BUCKETS.append(DEFAULT_CONFIG.s3_stage_bucket_id)

s3_fixture = s3_fixture_factory(
    existing_buckets=EXISTING_BUCKETS,
    non_existing_buckets=DEFAULT_NON_EXISTING_BUCKETS,
    existing_objects=DEFAULT_EXISTING_OBJECTS,
    non_existing_objects=DEFAULT_NON_EXISTING_OBJECTS,
)
