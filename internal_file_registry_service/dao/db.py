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

from datetime import datetime
from typing import Any, Optional

from ghga_service_chassis_lib.postgresql import (
    PostgresqlConfigBase,
    SyncPostgresqlConnector,
)
from ghga_service_chassis_lib.utils import DaoGenericBase
from sqlalchemy.future import select

from .. import models
from ..config import CONFIG
from . import db_models


class FileInfoNotFoundError(RuntimeError):
    """Thrown when trying to access info on a file with an external ID that doesn't
    exist in the database."""

    def __init__(self, file_id: str):
        message = (
            f"The information for the file with external ID '{file_id}' does not"
            + " exist in the database."
        )
        super().__init__(message)


class FileInfoAlreadyExistsError(RuntimeError):
    """Thrown when trying to create info for a file with an external ID that already
    exist in the database."""

    def __init__(self, file_id: Optional[str]):
        message = (
            f"The information for the file with external ID '{file_id}' already"
            + " exist in the database."
        )
        super().__init__(message)


# Since this is just a DAO stub without implementation, following pylint error are
# expected:
# pylint: disable=unused-argument,no-self-use
class DatabaseDao(DaoGenericBase):
    """
    A DAO base class for interacting with the database.

    It might throw following exception to communicate selected error events:
        - FileInfoNotFoundError
        - FileInfoAlreadyExistsError
    """

    def get_file_info(self, file_id: str) -> models.FileInfoComplete:
        """Get information for a file by specifying its external ID"""
        ...

    def register_file_info(self, file_info: models.FileInfoExternal) -> None:
        """Register information for a new to the database."""
        ...

    def unregister_file_info(self, file_id: str) -> None:
        """
        Unregister information for a file with the specified external ID from the database.
        """

        ...


class PostgresDatabase(DatabaseDao):
    """
    An implementation of the  DatabaseDao interface using a PostgreSQL backend.
    """

    def __init__(self, config: PostgresqlConfigBase = CONFIG):
        """initialze DAO implementation"""

        super().__init__(config=config)
        self._postgresql_connector = SyncPostgresqlConnector(config)

        # will be defined on __enter__:
        self._session_cm: Any = None
        self._session: Any = None

    def __enter__(self):
        """Setup database connection"""

        self._session_cm = self._postgresql_connector.transactional_session()
        self._session = self._session_cm.__enter__()  # pylint: disable=no-member
        return self

    def __exit__(self, error_type, error_value, error_traceback):
        """Teardown database connection"""
        # pylint: disable=no-member
        self._session_cm.__exit__(error_type, error_value, error_traceback)

    def _get_orm_file_info(self, file_id: str) -> db_models.FileInfo:
        """Internal method to get the ORM representation of a file info by specifying
        its external ID"""

        statement = select(db_models.FileInfo).filter_by(file_id=file_id)
        orm_file_info = self._session.execute(statement).scalars().one_or_none()

        if orm_file_info is None:
            raise FileInfoNotFoundError(file_id=file_id)

        return orm_file_info

    def get_file_info(self, file_id: str) -> models.FileInfoComplete:
        """Get information for a file by specifying its external ID"""

        orm_file_info = self._get_orm_file_info(file_id=file_id)
        return models.FileInfoComplete.from_orm(orm_file_info)

    def register_file_info(self, file_info: models.FileInfoExternal) -> None:
        """Register information for a new file to the database."""

        # check for collisions in the database:
        try:
            self._get_orm_file_info(file_id=file_info.file_id)
        except FileInfoNotFoundError:
            # this is expected
            pass
        else:
            # this is a problem
            raise FileInfoAlreadyExistsError(file_id=file_info.file_id)

        file_info_dict = {
            **file_info.dict(),
            "registration_date": datetime.now(),
        }
        orm_file_info = db_models.FileInfo(**file_info_dict)
        self._session.add(orm_file_info)

    def unregister_file_info(self, file_id: str) -> None:
        """
        Unregister information for a file with the specified external ID from the database.
        """

        orm_file_info = self._get_orm_file_info(file_id=file_id)
        self._session.delete(orm_file_info)
