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

"""Tests business-functionality of `core` subpackage"""

from internal_file_registry_service.core.main import copy_file_to_stage

from .fixtures import core_fixture  # noqa: F401


def test_copy_file(core_fixture):  # noqa: F811
    """Test copying of file"""

    copy_file_to_stage(
        external_file_id=core_fixture.existing_object.object_id,
        config=core_fixture.config,
    )

    assert core_fixture.storage.does_object_exist(
        object_id=core_fixture.existing_object.object_id,
        bucket_id=core_fixture.config.s3_stage_bucket_id,
    )
