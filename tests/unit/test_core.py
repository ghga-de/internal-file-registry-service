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

from typing import Optional, Type

import pytest

from internal_file_registry_service.core.main import (
    FileAlreadyInOutboxError,
    FileAlreadyInRegistryError,
    FileInDbButNotInStorageError,
    FileInStorageButNotInDbError,
    FileNotInInboxError,
    FileNotInRegistryError,
    register_file,
    stage_file,
)

from ..fixtures import get_config, psql_fixture, s3_fixture, state  # noqa: F401


@pytest.mark.parametrize(
    "file_name,expected_exception",
    [
        ("in_registry", None),
        ("in_registry_and_outbox", FileAlreadyInOutboxError),
        ("storage_missing", FileInDbButNotInStorageError),
        ("exists_nowhere", FileNotInRegistryError),
    ],
)
def test_stage_file(
    file_name: str,
    expected_exception: Optional[Type[Exception]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
):
    """Test copying of file to the out stage"""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    file = state.FILES[file_name]

    run = lambda: stage_file(external_file_id=file.id, config=config)

    if expected_exception is None:
        run()
        assert s3_fixture.storage.does_object_exist(
            object_id=file.id,
            bucket_id=config.s3_outbox_bucket_id,
        )
    else:
        with pytest.raises(expected_exception):
            run()


@pytest.mark.parametrize(
    "file_name,expected_exception",
    [
        ("in_inbox_only", None),
        ("exists_nowhere", FileNotInInboxError),
        ("in_registry", FileAlreadyInRegistryError),
        ("storage_missing", FileInDbButNotInStorageError),
        ("in_inbox_and_reg_but_db_missing", FileInStorageButNotInDbError),
    ],
)
def test_register_file(
    file_name: str,
    expected_exception: Optional[Type[BaseException]],
    psql_fixture,  # noqa: F811
    s3_fixture,  # noqa: F811
):
    """Test copying of file"""
    config = get_config(sources=[psql_fixture.config, s3_fixture.config])
    file = state.FILES[file_name]

    run = lambda: register_file(file_info=file.file_info, config=config)

    if expected_exception is None:
        run()
        assert s3_fixture.storage.does_object_exist(
            object_id=file.id,
            bucket_id=file.grouping_label,
        )
    else:
        with pytest.raises(expected_exception):
            run()
