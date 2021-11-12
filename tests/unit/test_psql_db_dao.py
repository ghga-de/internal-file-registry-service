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

"""Tests the database DAO implementation base on PostgreSQL"""

import pytest
from ghga_service_chassis_lib.postgresql import SyncPostgresqlConnector
from testcontainers.postgres import PostgresContainer

from internal_file_registry_service.dao.db import (
    FileObjectAlreadyExists,
    FileObjectNotFoundError,
    PostgresDatabase,
)

from .fixtures.psql_dao import (
    ADDITIONAL_FILE_FIXTURES,
    PREPOPULATED_FILE_FIXTURES,
    config_from_psql_container,
    populate_db,
)


def configure_database_dao(postgres: PostgresContainer) -> PostgresDatabase:
    """
    Get a PostgresDatabase DAO implementation configured for the provided
    PostgresContainer. Moreover, it will prepopulate the database with fixture entries.
    """
    config = config_from_psql_container(postgres)
    populate_db(config.db_url)
    psql_connector = SyncPostgresqlConnector(config)
    return PostgresDatabase(postgresql_connector=psql_connector)


def test_get_existing_file_obj():
    """Test getting exiting file object."""

    existing_file_obj = PREPOPULATED_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            returned_file_obj = database.get_file_object(existing_file_obj.external_id)

    assert existing_file_obj.md5_encrypted == returned_file_obj.md5_encrypted


def test_get_non_existing_file_obj():
    """Test getting not existing file object and expect corresponding error."""

    non_existing_file_obj = ADDITIONAL_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            with pytest.raises(FileObjectNotFoundError):
                database.get_file_object(non_existing_file_obj.external_id)


def test_register_non_existing_file_obj():
    """Test registering not existing file object."""

    non_existing_file_obj = ADDITIONAL_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            database.register_file_object(non_existing_file_obj)
            returned_file_obj = database.get_file_object(
                non_existing_file_obj.external_id
            )

    assert non_existing_file_obj.md5_encrypted == returned_file_obj.md5_encrypted


def test_register_existing_file_obj():
    """Test registering an already existing file object and expect corresponding
    error."""

    existing_file_obj = PREPOPULATED_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            with pytest.raises(FileObjectAlreadyExists):
                database.register_file_object(existing_file_obj)


def test_unregister_non_existing_file_obj():
    """Test unregistering not existing file object and expect corresponding error."""

    non_existing_file_obj = ADDITIONAL_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            with pytest.raises(FileObjectNotFoundError):
                database.unregister_file_object(non_existing_file_obj.external_id)


def test_unregister_existing_file_obj():
    """Test unregistering an existing file object."""

    existing_file_obj = PREPOPULATED_FILE_FIXTURES[0]

    with PostgresContainer() as postgres:
        with configure_database_dao(postgres) as database:
            database.unregister_file_object(existing_file_obj.external_id)

            # check if file object can no longer be found:
            with pytest.raises(FileObjectNotFoundError):
                database.get_file_object(existing_file_obj.external_id)
