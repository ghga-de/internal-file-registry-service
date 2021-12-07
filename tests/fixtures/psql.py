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

"""Fixtures for testing the PostgreSQL functionalities"""

from dataclasses import dataclass
from datetime import datetime
from typing import Generator, List

import pytest
from ghga_service_chassis_lib.object_storage_dao_testing import (
    DEFAULT_NON_EXISTING_OBJECTS,
)
from ghga_service_chassis_lib.postgresql import PostgresqlConfigBase
from ghga_service_chassis_lib.postgresql_testing import config_from_psql_container
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from internal_file_registry_service import models
from internal_file_registry_service.dao import db_models
from internal_file_registry_service.dao.db import PostgresDatabase

from .config import DEFAULT_CONFIG
from .storage import EXISTING_OBJECTS

EXISTING_FILE_INFOS = [
    models.FileInfoExternal(
        external_id=existing_object.object_id,
        grouping_label=existing_object.bucket_id,
        md5_checksum=existing_object.md5,
        size=1000,  # not the real size
    )
    for existing_object in EXISTING_OBJECTS
    # Omit objects that are located in the stage bucket
    # as they are not tracked by this service:
    if existing_object.bucket_id != DEFAULT_CONFIG.s3_out_stage_bucket_id
]

NON_EXISTING_FILE_INFOS = [
    models.FileInfoExternal(
        external_id=non_existing_object.object_id,
        grouping_label=non_existing_object.bucket_id,
        md5_checksum=non_existing_object.md5,
        size=1000,  # not the real size
    )
    for non_existing_object in DEFAULT_NON_EXISTING_OBJECTS
]


def populate_db(db_url: str, existing_file_infos: List[models.FileInfoExternal]):
    """Create and populates the DB"""

    # setup database and tables:
    engine = create_engine(db_url)
    db_models.Base.metadata.create_all(engine)

    # populate with test data:
    session_factor = sessionmaker(engine)
    with session_factor() as session:
        for existing_file_info in existing_file_infos:
            param_dict = {
                **existing_file_info.dict(),
                "registration_date": datetime.now(),
            }
            orm_entry = db_models.FileInfo(**param_dict)
            session.add(orm_entry)
        session.commit()


@dataclass
class PsqlFixture:
    """Info yielded by the `psql_fixture` function"""

    config: PostgresqlConfigBase
    database: PostgresDatabase
    existing_file_infos: List[models.FileInfoExternal]
    non_existing_file_infos: List[models.FileInfoExternal]


@pytest.fixture
def psql_fixture() -> Generator[PsqlFixture, None, None]:
    """Pytest fixture for tests of the Prostgres DAO implementation."""

    with PostgresContainer() as postgres:
        config = config_from_psql_container(postgres)
        populate_db(config.db_url, existing_file_infos=EXISTING_FILE_INFOS)

        with PostgresDatabase(config) as database:
            yield PsqlFixture(
                config=config,
                database=database,
                existing_file_infos=EXISTING_FILE_INFOS,
                non_existing_file_infos=NON_EXISTING_FILE_INFOS,
            )
