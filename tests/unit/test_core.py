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

from internal_file_registry_service.core.main import register_file, stage_file

from ..fixtures import FILE_FIXTURES, get_config, psql_fixture, s3_fixture  # noqa: F401


def test_stage_file(psql_fixture, s3_fixture):  # noqa: F811
    """Test copying of file to the out stage"""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    existing_object_id = FILE_FIXTURES["in_registry"].id

    stage_file(
        external_file_id=existing_object_id,
        config=config,
    )

    assert s3_fixture.storage.does_object_exist(
        object_id=existing_object_id,
        bucket_id=config.s3_outbox_bucket_id,
    )


def test_register_file(psql_fixture, s3_fixture):  # noqa: F811
    """Test copying of file"""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    file_fixture = FILE_FIXTURES["in_inbox_only"]

    register_file(
        file_info=file_fixture.file_info,
        config=config,
    )

    assert s3_fixture.storage.does_object_exist(
        object_id=file_fixture.id,
        bucket_id=config.s3_outbox_bucket_id,
    )
