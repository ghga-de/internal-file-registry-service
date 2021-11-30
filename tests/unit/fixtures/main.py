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

"""Main pytest fixture functions."""


from dataclasses import dataclass
from typing import Generator

import pytest
from ghga_service_chassis_lib.object_storage_dao_testing import populate_storage
from testcontainers.localstack import LocalStackContainer
from testcontainers.postgres import PostgresContainer

from internal_file_registry_service import models
from internal_file_registry_service.config import Config
from internal_file_registry_service.dao import ObjectStorage
from internal_file_registry_service.dao.db import PostgresDatabase

from .config import get_config
from .psql_dao import EXISTING_FILE_INFO, NOT_EXISTING_FILE_INFO, populate_db
from .storage import EXISTING_OBJECT, NOT_EXISTING_OBJECT, ObjectFixture


@dataclass
class PsqlFixture:
    """Info yielded by the `psql_fixture` function"""

    config: Config
    database: PostgresDatabase
    existing_file_info: models.FileInfoExternal
    not_existing_file_info: models.FileInfoExternal


@pytest.fixture
def psql_fixture() -> Generator[PsqlFixture, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with PostgresContainer() as postgres:
        config = get_config(psql_container=postgres)
        populate_db(config.db_url, fixtures=[EXISTING_FILE_INFO])

        with PostgresDatabase(config) as database:
            yield PsqlFixture(
                config=config,
                database=database,
                existing_file_info=EXISTING_FILE_INFO,
                not_existing_file_info=NOT_EXISTING_FILE_INFO,
            )


@dataclass
class CoreFixture:
    """Info yielded by the `core_fixture` function"""

    config: Config
    database: PostgresDatabase
    storage: ObjectStorage
    existing_object: ObjectFixture
    not_existing_object: ObjectFixture


@pytest.fixture
def core_fixture() -> Generator[CoreFixture, None, None]:
    """Pytest fixture for tests of the core module."""

    with PostgresContainer() as postgres:
        with LocalStackContainer() as localstack:
            config = get_config(
                localstack_container=localstack, psql_container=postgres
            )
            populate_db(config.db_url, fixtures=[EXISTING_FILE_INFO])

            with ObjectStorage(config=config) as storage:
                populate_storage(storage=storage, fixtures=[EXISTING_OBJECT])
                storage.create_bucket(config.s3_stage_bucket_id)

                with PostgresDatabase(config) as database:
                    yield CoreFixture(
                        config=config,
                        database=database,
                        storage=storage,
                        existing_object=EXISTING_OBJECT,
                        not_existing_object=NOT_EXISTING_OBJECT,
                    )
