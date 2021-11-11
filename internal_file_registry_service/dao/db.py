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

"""Database DAO"""

from typing import Optional

from ghga_service_chassis_lib.postgresql import PostgresqlConnector
from ghga_service_chassis_lib.utils import DaoGenericBase
from sqlalchemy.future import select

from .. import models
from ..config import config
from . import db_models

psql_connector = PostgresqlConnector(config)


class FileObjectNotFoundError(RuntimeError):
    """Thrown when trying to access a file object with an external ID that doesn't
    exist in the database."""

    def __init__(self, external_id: Optional[str]):
        message = (
            "The file object "
            + (f"with external ID '{external_id}' " if external_id else "")
            + "does not exist in the database."
        )
        super().__init__(message)


class FileObjectAlreadyExists(RuntimeError):
    """Thrown when trying to create a file object with an external ID that already
    exist in the database."""

    def __init__(self, external_id: Optional[str]):
        message = (
            "The file object "
            + (f"with external ID '{external_id}' " if external_id else "")
            + "already exist in the database."
        )
        super().__init__(message)


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.

    It might throw following exception to communicate selected error events:
        - FileObjectNotFoundError
        - FileObjectAlreadyExists
    """

    def get_file_object(self, external_id: str) -> models.FileObjectComplete:
        """Get file object information by specifying its external ID"""
        ...

    def register_file_object(self, file_object: models.FileObjectWithoutID) -> None:
        """Register a new file object to the database."""
        ...

    def unregister_file_object(self, external_id: str) -> None:
        """Unregister a file object from the database specifying its external ID."""
        ...


class PostgresDatabase(DatabaseDao):
    """
    An implementation of the  DatabaseDao interface using a PostgreSQL backend.
    """

    def __init__(self):
        """initialze DAO implementation"""

        super().__init__()

        # will be defined on __enter__:
        self._session_cm = None
        self._session = None

    def __enter__(self):
        """Setup database connection"""

        self._session_cm = psql_connector.transactional_session()
        self._session = self._session_cm.__enter__()
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        """Teardown database connection"""

        self._session_cm.__exit__(error_type, error_value, error_traceback)

    def _get_orm_file_object(self, external_id: str) -> db_models.FileObject:
        """Internal method to get the ORM representation of a file object by specifying
        its external ID"""

        statement = select(db_models.FileObject).filter_by(external_id=external_id)
        orm_file_object = self._session.execute(statement).scalars().one_or_none()

        if orm_file_object is None:
            raise FileObjectNotFoundError(external_id=external_id)

        return orm_file_object

    def get_file_object(self, external_id: str) -> models.FileObjectComplete:
        """Get file object information by specifying its external ID"""

        orm_file_object = self._get_orm_file_object(external_id=external_id)
        return models.FileObjectComplete.from_orm(orm_file_object)

    def register_file_object(self, file_object: models.FileObjectWithoutID) -> None:
        """Register a new file object to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_file_object(external_id=file_object.external_id)
        except FileObjectNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise FileObjectAlreadyExists(external_id=file_object.external_id)

        orm_file_object = db_models.FileObject(**file_object.dict())
        self._session.add(orm_file_object)

    def unregister_file_object(self, external_id: str) -> None:
        """Unregister a file object from the database specifying its external ID."""

        orm_file_object = self._get_orm_file_object(external_id=external_id)
        self._session.delete(orm_file_object)
